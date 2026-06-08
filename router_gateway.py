import socket
import re
import logging
import time
from urllib.parse import urlparse
from xml.dom.minidom import parseString, Document
from typing import Optional
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)

# When you launch a UPnP-enabled application
    # Discovery: The device joins the network and shouts, "Is there a router here that speaks UPnP?"
    # Description: The router responds with its capabilities.
    # Request: The device says, "I need to receive data on Port 3074. Can you open that for me and send it to my IP address?"
    # Action: The router automatically creates a temporary Port Forwarding rule.

@dataclass
class SoapResult:
    status_code: int
    body: bytes


@dataclass
class GatewayService:
    control_path: str
    service_type: str


class SSDPScanner:

    _MULTICAST_ADDR = "239.255.255.250"
    _MULTICAST_PORT = 1900
    _SEARCH_TARGET = "upnp:rootdevice"

    def __init__(self, scan_timeout: float = 0.1):
        self._scan_timeout = scan_timeout

    def _build_search_packet(self) -> bytes:
        lines = [
            "M-SEARCH * HTTP/1.1",
            f"HOST:{self._MULTICAST_ADDR}:{self._MULTICAST_PORT}",
            f"ST:{self._SEARCH_TARGET}",
            "MX:2",
            'MAN:"ssdp:discover"',
            "",
            "",
        ]
        return "\r\n".join(lines).encode()

    def _extract_location(self, raw_response: bytes) -> Optional[str]:
        decoded = raw_response.decode("utf-8", errors="ignore")
        headers = re.findall(r"(?P<name>.*?): (?P<value>.*?)\r\n", decoded)
        matches = [val for name, val in headers if name.lower() == "location"]
        return matches[0] if matches else None

    def scan_for_gateways(self) -> list[str]:
        udp_sock = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
        )
        packet = self._build_search_packet()
        udp_sock.sendto(packet, (self._MULTICAST_ADDR, self._MULTICAST_PORT))
        udp_sock.settimeout(self._scan_timeout)

        discovered_paths: list[str] = []
        while True:
            try:
                raw_data, _ = udp_sock.recvfrom(1024)
                location = self._extract_location(raw_data)
                if location:
                    discovered_paths.append(location)
                break
            except socket.error:
                break

        udp_sock.close()
        return discovered_paths


class GatewayProfileParser:

    @staticmethod
    def fetch_service_info(profile_url: str) -> Optional[GatewayService]:
        try:
            raw_xml = httpx.get(profile_url, timeout=5.0).content
        except httpx.RequestError as exc:
            logger.error("Failed to fetch gateway profile: %s", exc)
            return None

        dom = parseString(raw_xml)
        service_nodes = dom.getElementsByTagName("serviceType")

        for node in service_nodes:
            type_string = node.childNodes[0].data
            is_wan_ip = "WANIPConnection" in type_string
            is_wan_ppp = "WANPPPConnection" in type_string

            if is_wan_ip or is_wan_ppp:
                parent = node.parentNode
                ctrl_elements = parent.getElementsByTagName("controlURL")
                ctrl_path = ctrl_elements[0].childNodes[0].data
                return GatewayService(
                    control_path=ctrl_path,
                    service_type=type_string,
                )

        return None


class SoapRequestBuilder:

    _ENVELOPE_NS = "http://schemas.xmlsoap.org/soap/envelope/"
    _ENCODING_STYLE = "http://schemas.xmlsoap.org/soap/encoding/"

    def build_port_mapping_xml(
        self,
        action_name: str,
        wan_service: str,
        ext_port: int,
        int_port: int,
        target_host: str,
        protocol: str = "TCP",
        is_enabled: int = 1,
        lease_seconds: int = 0,
        label: Optional[str] = None,
    ) -> str:
        if label is None:
            label = "auto-generated-mapping"

        if not is_enabled:
            lease_seconds = 1

        doc = Document()

        envelope = doc.createElementNS("", "s:Envelope")
        envelope.setAttribute("xmlns:s", self._ENVELOPE_NS)
        envelope.setAttribute("s:encodingStyle", self._ENCODING_STYLE)

        body_elem = doc.createElementNS("", "s:Body")

        action_elem = doc.createElementNS("", f"u:{action_name}")
        action_elem.setAttribute("xmlns:u", wan_service)

        field_pairs = [
            ("NewRemoteHost", ""),
            ("NewExternalPort", ext_port),
            ("NewProtocol", protocol),
            ("NewInternalPort", int_port),
            ("NewInternalClient", target_host),
            ("NewEnabled", is_enabled),
            ("NewPortMappingDescription", label),
            ("NewLeaseDuration", lease_seconds),
        ]

        for tag_name, tag_value in field_pairs:
            elem = doc.createElement(tag_name)
            text = doc.createTextNode(str(tag_value))
            elem.appendChild(text)
            action_elem.appendChild(elem)

        body_elem.appendChild(action_elem)
        envelope.appendChild(body_elem)
        doc.appendChild(envelope)

        return doc.toxml()


class SoapTransport:

    def send_request(
        self,
        service_url: str,
        wan_service: str,
        action_name: str,
        xml_payload: str,
    ) -> SoapResult:
        parsed = urlparse(service_url)
        conn = None
        try:
            conn = httpx.Client()
            full_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}{parsed.path}"
            soap_action = f"{wan_service}#{action_name}"
            resp = conn.post(
                full_url,
                content=xml_payload,
                headers={
                    "SOAPAction": soap_action,
                    "Content-Type": "text/xml",
                },
                timeout=10.0,
            )
            return SoapResult(
                status_code=resp.status_code,
                body=resp.content,
            )
        except httpx.RequestError as exc:
            logger.error("SOAP transport error: %s", exc)
            return SoapResult(status_code=500, body=str(exc).encode())
        finally:
            if conn:
                conn.close()


class LANAddressResolver:

    @staticmethod
    def resolve(gateway_ip: Optional[str] = None) -> Optional[str]:
        if gateway_ip is None:
            gateway_ip = "8.8.8.8"

        try:
            probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            probe.connect((gateway_ip, 80))
            local_addr = probe.getsockname()[0]
            probe.close()
            return local_addr
        except socket.error:
            return None


class RouterPortManager:

    _ADD_ACTION = "AddPortMapping"
    _DELETE_ACTION = "DeletePortMapping"
    _QUERY_ACTION = "GetSpecificPortMappingEntry"

    def __init__(
        self,
        scanner: Optional[SSDPScanner] = None,
        profile_parser: Optional[GatewayProfileParser] = None,
        soap_builder: Optional[SoapRequestBuilder] = None,
        soap_transport: Optional[SoapTransport] = None,
        lan_resolver: Optional[LANAddressResolver] = None,
        fallback_paths: Optional[list[str]] = None,
        discovery_timeout: float = 5.0,
    ):
        self._scanner = scanner or SSDPScanner()
        self._parser = profile_parser or GatewayProfileParser()
        self._builder = soap_builder or SoapRequestBuilder()
        self._transport = soap_transport or SoapTransport()
        self._lan_resolver = lan_resolver or LANAddressResolver()
        self._fallback_paths = fallback_paths or []
        self._discovery_timeout = discovery_timeout

    def _discover_gateways(self) -> list[str]:
        found = self._scanner.scan_for_gateways()

        deadline = time.time() + self._discovery_timeout
        while len(found) == 0 and time.time() < deadline:
            found = self._scanner.scan_for_gateways()

        if not found and self._fallback_paths:
            return list(self._fallback_paths)

        return found

    def _resolve_service(self, profile_url: str):
        parsed = urlparse(profile_url)
        gateway_info = self._parser.fetch_service_info(profile_url)
        if gateway_info is None:
            return None, None, None, None

        service_url = (
            f"{parsed.scheme}://{parsed.netloc}{gateway_info.control_path}"
        )
        router_ip = parsed.netloc.split(":")[0]
        return gateway_info.service_type, service_url, router_ip, gateway_info

    def _execute_on_gateway(
        self,
        action: str,
        profile_url: str,
        ext_port: int,
        int_port: int,
        lan_ip: Optional[str],
        router_filter: Optional[str],
        protocol: str,
        lease: int,
        label: Optional[str],
        enabled_flag: int,
        verbose: bool,
    ) -> Optional[bool]:
        wan_service, svc_url, gw_ip, _ = self._resolve_service(profile_url)
        if wan_service is None:
            return None

        if router_filter is not None and gw_ip not in router_filter:
            return None

        target_ip = lan_ip
        if target_ip is None:
            target_ip = self._lan_resolver.resolve(gw_ip)

        xml = self._builder.build_port_mapping_xml(
            action_name=action,
            wan_service=wan_service,
            ext_port=ext_port,
            int_port=int_port,
            target_host=target_ip,
            protocol=protocol,
            is_enabled=enabled_flag,
            lease_seconds=lease,
            label=label,
        )

        result = self._transport.send_request(svc_url, wan_service, action, xml)

        if result.status_code == 200:
            if verbose:
                prefix = "disable of " if not enabled_flag else ""
                logger.info(
                    "%sport forward on %s OK, %s->%s:%s",
                    prefix, gw_ip, ext_port, target_ip, int_port,
                )
            return True

        logger.warning(
            "Port action '%s' failed on %s (HTTP %d)",
            action, gw_ip, result.status_code,
        )
        return False

    def forward_port(
        self,
        eport: int,
        iport: int,
        router: Optional[str] = None,
        lanip: Optional[str] = None,
        disable: bool = False,
        protocol: str = "TCP",
        duration: int = 0,
        description: Optional[str] = None,
        verbose: bool = False,
    ) -> bool:
        if verbose:
            logger.info("Discovering routers...")

        gateway_urls = self._discover_gateways()
        enabled_value = int(not disable)
        action = self._DELETE_ACTION if disable else self._ADD_ACTION

        any_success = False
        for url in gateway_urls:
            outcome = self._execute_on_gateway(
                action=action,
                profile_url=url,
                ext_port=eport,
                int_port=iport,
                lan_ip=lanip,
                router_filter=router,
                protocol=protocol,
                lease=duration,
                label=description,
                enabled_flag=enabled_value,
                verbose=verbose,
            )
            if outcome is True:
                any_success = True

        return any_success

    def is_port_open(
        self,
        ext_port: int,
        int_port: Optional[int] = None,
        router: Optional[str] = None,
        lanip: Optional[str] = None,
        disable: bool = False,
        protocol: str = "TCP",
        duration: int = 0,
        description: Optional[str] = None,
    ) -> bool:
        gateway_urls = self._discover_gateways()
        enabled_value = int(not disable)

        mapped = False
        for url in gateway_urls:
            outcome = self._execute_on_gateway(
                action=self._QUERY_ACTION,
                profile_url=url,
                ext_port=ext_port,
                int_port=int_port if int_port else ext_port,
                lan_ip=lanip,
                router_filter=router,
                protocol=protocol,
                lease=duration,
                label=description,
                enabled_flag=enabled_value,
                verbose=False,
            )
            if outcome is True:
                mapped = True
            elif outcome is False:
                mapped = False

        return mapped

    def get_my_ip(self, gateway_ip: Optional[str] = None) -> Optional[str]:
        return self._lan_resolver.resolve(gateway_ip)