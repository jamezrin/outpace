//! UPnP device-description parsing for media renderers.
//!
//! The XML is untrusted: it is size-capped before parsing, parsed without DTDs under a node
//! limit, walked only to a fixed device-nesting depth, and its control URL must stay on the
//! host and port that served the description.

use super::url::LanUrl;
use super::CastError;

/// Largest device description accepted.
pub const MAX_DESCRIPTION: usize = 64 * 1024;
const MAX_NODES: u32 = 4096;
/// `root/device/deviceList/device/...`: embedded devices deeper than this are ignored.
const MAX_ELEMENT_DEPTH: usize = 12;
const MAX_TEXT: usize = 128;
const AV_TRANSPORT_PREFIX: &str = "urn:schemas-upnp-org:service:AVTransport:";

/// A renderer's AVTransport endpoint and display details.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Renderer {
    pub udn: Option<String>,
    pub name: String,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    /// Validated `urn:schemas-upnp-org:service:AVTransport:<n>`.
    pub service_type: String,
    pub control: LanUrl,
}

/// Parse `xml` fetched from `location` into the first device offering AVTransport.
pub fn parse(xml: &[u8], location: &LanUrl) -> Result<Renderer, CastError> {
    if xml.len() > MAX_DESCRIPTION {
        return Err(CastError::TooLarge);
    }
    let text = std::str::from_utf8(xml).map_err(|_| CastError::Malformed("bad description"))?;
    let document = roxmltree::Document::parse_with_options(
        text,
        roxmltree::ParsingOptions {
            allow_dtd: false,
            nodes_limit: MAX_NODES,
            ..Default::default()
        },
    )
    .map_err(|_| CastError::Malformed("bad description xml"))?;
    let root = document.root_element();
    if root.tag_name().name() != "root" {
        return Err(CastError::Malformed("description root is not <root>"));
    }
    let base = child(root, "URLBase")
        .and_then(|node| node.text())
        .and_then(|text| location.join(text).ok())
        .unwrap_or_else(|| location.clone());
    let root_device = child(root, "device").ok_or(CastError::Malformed("no device"))?;

    let device = root_device
        .descendants()
        .filter(|node| node.is_element() && node.tag_name().name() == "device")
        .filter(|node| node.ancestors().count() <= MAX_ELEMENT_DEPTH)
        .find_map(|device| av_transport(device).map(|service| (device, service)));
    let Some((device, (service_type, control))) = device else {
        return Err(CastError::Malformed("no AVTransport service"));
    };
    // `URLBase` and the control URL may be absolute (or `//host`), so check the result: the
    // control endpoint must be the very host and port that served this description.
    let control = base.join(control)?;
    if control.addr != location.addr {
        return Err(CastError::Forbidden("control url is on another host"));
    }
    let field = |name| {
        clean(child(device, name).and_then(|n| n.text()))
            .or_else(|| clean(child(root_device, name).and_then(|n| n.text())))
    };
    Ok(Renderer {
        udn: field("UDN"),
        name: field("friendlyName").unwrap_or_else(|| "Unnamed renderer".to_string()),
        manufacturer: field("manufacturer"),
        model: field("modelName"),
        service_type: service_type.to_string(),
        control,
    })
}

/// The AVTransport `(serviceType, controlURL)` declared directly by `device`, if any.
fn av_transport<'a>(device: roxmltree::Node<'a, 'a>) -> Option<(&'a str, &'a str)> {
    let services = child(device, "serviceList")?;
    services
        .children()
        .filter(|node| node.is_element() && node.tag_name().name() == "service")
        .find_map(|service| {
            let service_type = child(service, "serviceType")?.text()?.trim();
            let version = service_type.strip_prefix(AV_TRANSPORT_PREFIX)?;
            if version.is_empty()
                || version.len() > 2
                || !version.bytes().all(|b| b.is_ascii_digit())
            {
                return None;
            }
            let control = child(service, "controlURL")?.text()?.trim();
            (!control.is_empty()).then_some((service_type, control))
        })
}

fn child<'a>(node: roxmltree::Node<'a, 'a>, name: &str) -> Option<roxmltree::Node<'a, 'a>> {
    node.children()
        .find(|child| child.is_element() && child.tag_name().name() == name)
}

/// Trim, drop control characters, and cap a display string.
fn clean(text: Option<&str>) -> Option<String> {
    let cleaned: String = text?
        .trim()
        .chars()
        .filter(|c| !c.is_control())
        .take(MAX_TEXT)
        .collect();
    (!cleaned.is_empty()).then_some(cleaned)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector(name: &str) -> Vec<u8> {
        let path = format!(
            "{}/../../tests/vectors/dlna/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read(path).unwrap()
    }

    fn location() -> LanUrl {
        LanUrl::parse("http://192.168.1.40:49152/description.xml").unwrap()
    }

    #[test]
    fn parses_a_tv_description() {
        let renderer = parse(&vector("renderer-tv.xml"), &location()).unwrap();
        assert_eq!(renderer.name, "Living Room TV");
        assert_eq!(renderer.manufacturer.as_deref(), Some("Example Corp"));
        assert_eq!(renderer.model.as_deref(), Some("ExampleTV 55"));
        assert_eq!(
            renderer.udn.as_deref(),
            Some("uuid:00000000-0000-0000-0000-000000000001")
        );
        assert_eq!(
            renderer.service_type,
            "urn:schemas-upnp-org:service:AVTransport:1"
        );
        assert_eq!(
            renderer.control,
            LanUrl::parse("http://192.168.1.40:49152/upnp/control/AVTransport1").unwrap()
        );
    }

    #[test]
    fn finds_av_transport_on_an_embedded_device_with_url_base() {
        let renderer = parse(&vector("renderer-embedded.xml"), &location()).unwrap();
        assert_eq!(renderer.name, "Software Renderer");
        assert_eq!(
            renderer.service_type,
            "urn:schemas-upnp-org:service:AVTransport:2"
        );
        assert_eq!(
            renderer.control,
            LanUrl::parse("http://192.168.1.40:49152/dev/AVTransport/control").unwrap()
        );
    }

    #[test]
    fn rejects_a_control_url_on_another_port_or_via_url_base() {
        let device = |url_base: &str, control: &str| {
            format!(
                "<root>{url_base}<device><serviceList><service>\
                 <serviceType>urn:schemas-upnp-org:service:AVTransport:1</serviceType>\
                 <controlURL>{control}</controlURL></service></serviceList></device></root>"
            )
        };
        let forbidden = Err(CastError::Forbidden("control url is on another host"));
        for (url_base, control) in [
            ("", "http://192.168.1.40:8080/ctl"),
            ("", "//192.168.1.40:22/ctl"),
            ("", "//192.168.1.1/ctl"),
            ("<URLBase>http://192.168.1.40:1900/</URLBase>", "ctl"),
            ("<URLBase>http://192.168.1.1:49152/</URLBase>", "/ctl"),
        ] {
            let xml = device(url_base, control);
            assert_eq!(parse(xml.as_bytes(), &location()), forbidden, "{xml}");
        }
        let ok = device("<URLBase>http://192.168.1.40:49152/a/</URLBase>", "ctl");
        assert_eq!(
            parse(ok.as_bytes(), &location()).unwrap().control.path,
            "/a/ctl"
        );
    }

    #[test]
    fn rejects_a_control_url_on_another_host() {
        assert_eq!(
            parse(&vector("renderer-foreign-control.xml"), &location()),
            Err(CastError::Forbidden("control url is on another host"))
        );
    }

    #[test]
    fn rejects_devices_without_av_transport_and_hostile_xml() {
        assert_eq!(
            parse(&vector("renderer-no-avtransport.xml"), &location()),
            Err(CastError::Malformed("no AVTransport service"))
        );
        // DTDs (and so entity expansion) are refused outright.
        assert!(parse(&vector("renderer-dtd.xml"), &location()).is_err());
        let oversize = vec![b' '; MAX_DESCRIPTION + 1];
        assert_eq!(parse(&oversize, &location()), Err(CastError::TooLarge));
        let mut deep = String::from("<root><device>");
        for _ in 0..5000 {
            deep.push_str("<a>");
        }
        assert!(parse(deep.as_bytes(), &location()).is_err());
        assert!(parse(b"<html/>", &location()).is_err());
    }

    #[test]
    fn display_fields_are_cleaned_and_capped() {
        let xml = format!(
            "<root><device><friendlyName>  TV\t{}  </friendlyName><serviceList><service>\
             <serviceType>urn:schemas-upnp-org:service:AVTransport:1</serviceType>\
             <controlURL>/c</controlURL></service></serviceList></device></root>",
            "x".repeat(500)
        );
        let renderer = parse(xml.as_bytes(), &location()).unwrap();
        assert!(renderer.name.starts_with("TVx"));
        assert_eq!(renderer.name.chars().count(), MAX_TEXT);
        assert_eq!(renderer.manufacturer, None);
    }

    #[test]
    fn rejects_unexpected_service_type_suffixes() {
        let xml = "<root><device><serviceList><service>\
             <serviceType>urn:schemas-upnp-org:service:AVTransport:1\"#Evil</serviceType>\
             <controlURL>/c</controlURL></service></serviceList></device></root>";
        assert!(parse(xml.as_bytes(), &location()).is_err());
    }
}
