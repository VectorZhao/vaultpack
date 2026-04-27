from dataclasses import dataclass
from urllib.parse import quote, urljoin
from xml.etree import ElementTree

import requests


@dataclass
class WebDAVConfig:
    base_url: str
    username: str
    password: str
    remote_dir: str


class WebDAVClient:
    def __init__(self, config: WebDAVConfig, timeout=60):
        self.config = config
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = (config.username, config.password)

    def _url(self, path=""):
        return self._url_for_parts(self.config.remote_dir, path)

    def _url_for_parts(self, *paths):
        base = self.config.base_url.rstrip("/") + "/"
        pieces = []
        for path in paths:
            pieces.extend(quote(part) for part in path.strip("/").split("/") if part)
        return urljoin(base, "/".join(pieces))

    def test(self):
        response = self.session.request("PROPFIND", self._url(), headers={"Depth": "0"}, timeout=self.timeout)
        if response.status_code in (200, 207):
            return
        if response.status_code == 404:
            self.ensure_remote_dir()
            return
        response.raise_for_status()

    def ensure_remote_dir(self):
        current = ""
        for part in self.config.remote_dir.strip("/").split("/"):
            if not part:
                continue
            current = f"{current}/{part}" if current else part
            response = self.session.request("MKCOL", self._url_for_parts(current), timeout=self.timeout)
            if response.status_code not in (201, 405):
                response.raise_for_status()

    def upload_file(self, local_path, remote_name):
        self.ensure_remote_dir()
        with open(local_path, "rb") as handle:
            response = self.session.put(self._url(remote_name), data=handle, timeout=None)
        response.raise_for_status()

    def delete(self, remote_name):
        response = self.session.delete(self._url(remote_name), timeout=self.timeout)
        if response.status_code not in (200, 204, 404):
            response.raise_for_status()

    def list_files(self):
        response = self.session.request("PROPFIND", self._url(), headers={"Depth": "1"}, timeout=self.timeout)
        if response.status_code == 404:
            return []
        response.raise_for_status()
        return _parse_propfind(response.text)


def _parse_propfind(xml_text):
    ns = {"d": "DAV:"}
    root = ElementTree.fromstring(xml_text)
    files = []
    for item in root.findall("d:response", ns):
        href = item.findtext("d:href", default="", namespaces=ns)
        is_collection = item.find(".//d:collection", ns) is not None
        if is_collection:
            continue
        name = href.rstrip("/").split("/")[-1]
        if name:
            files.append(name)
    return files
