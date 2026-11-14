//! AVTransport SOAP request builders and response parsers.

use super::CastError;

/// Largest SOAP response accepted.
pub const MAX_SOAP_RESPONSE: usize = 16 * 1024;
const MAX_NODES: u32 = 512;
const MAX_VALUE: usize = 64;

/// Escape text for XML element content and attribute values.
pub fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c if c.is_control() && c != '\t' && c != '\n' && c != '\r' => {}
            c => out.push(c),
        }
    }
    out
}

/// The `SOAPACTION` header value for `action` on `service_type`.
pub fn soap_action(service_type: &str, action: &str) -> String {
    format!("\"{service_type}#{action}\"")
}

/// A SOAP envelope invoking `action` with `args` (values are escaped here).
pub fn envelope(service_type: &str, action: &str, args: &[(&str, &str)]) -> String {
    let mut body = String::new();
    for (name, value) in args {
        body.push('<');
        body.push_str(name);
        body.push('>');
        body.push_str(&escape(value));
        body.push_str("</");
        body.push_str(name);
        body.push('>');
    }
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
         <s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" \
         s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"><s:Body>\
         <u:{action} xmlns:u=\"{service_type}\">{body}</u:{action}></s:Body></s:Envelope>"
    )
}

/// MPEG-TS MIME types offered to a renderer, in order: the registered type first, then the
/// type DLNA's `MPEG_TS_*` media profiles use, which many renderers list instead.
pub const TS_MIME_TYPES: [&str; 2] = ["video/mp2t", "video/mpeg"];

/// UPnP AVTransport error 714, "Illegal MIME-type".
pub const ILLEGAL_MIME_TYPE: u32 = 714;

/// Minimal DIDL-Lite for one live MPEG-TS video item. `mime` is one of [`TS_MIME_TYPES`].
pub fn didl_lite(title: &str, url: &str, mime: &str) -> String {
    format!(
        "<DIDL-Lite xmlns=\"urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/\" \
         xmlns:dc=\"http://purl.org/dc/elements/1.1/\" \
         xmlns:upnp=\"urn:schemas-upnp-org:metadata-1-0/upnp/\">\
         <item id=\"0\" parentID=\"-1\" restricted=\"1\"><dc:title>{}</dc:title>\
         <upnp:class>object.item.videoItem</upnp:class>\
         <res protocolInfo=\"http-get:*:{}:*\">{}</res></item></DIDL-Lite>",
        escape(title),
        escape(mime),
        escape(url)
    )
}

pub fn set_av_transport_uri(service_type: &str, url: &str, title: &str, mime: &str) -> String {
    envelope(
        service_type,
        "SetAVTransportURI",
        &[
            ("InstanceID", "0"),
            ("CurrentURI", url),
            ("CurrentURIMetaData", &didl_lite(title, url, mime)),
        ],
    )
}

pub fn play(service_type: &str) -> String {
    envelope(service_type, "Play", &[("InstanceID", "0"), ("Speed", "1")])
}

pub fn stop(service_type: &str) -> String {
    envelope(service_type, "Stop", &[("InstanceID", "0")])
}

pub fn get_transport_info(service_type: &str) -> String {
    envelope(service_type, "GetTransportInfo", &[("InstanceID", "0")])
}

/// `GetTransportInfo` output arguments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportInfo {
    pub state: String,
    pub status: Option<String>,
    pub speed: Option<String>,
}

/// A UPnP error carried by a SOAP fault.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fault {
    pub code: Option<u32>,
    pub description: Option<String>,
}

fn document(xml: &[u8]) -> Result<roxmltree::Document<'_>, CastError> {
    if xml.len() > MAX_SOAP_RESPONSE {
        return Err(CastError::TooLarge);
    }
    let text = std::str::from_utf8(xml).map_err(|_| CastError::Malformed("bad soap response"))?;
    roxmltree::Document::parse_with_options(
        text,
        roxmltree::ParsingOptions {
            allow_dtd: false,
            nodes_limit: MAX_NODES,
            ..Default::default()
        },
    )
    .map_err(|_| CastError::Malformed("bad soap xml"))
}

/// Text of the first element named `name` (namespace ignored), cleaned and capped.
fn value(doc: &roxmltree::Document<'_>, name: &str) -> Option<String> {
    let text = doc
        .descendants()
        .find(|node| node.is_element() && node.tag_name().name() == name)?
        .text()?
        .trim();
    let cleaned: String = text
        .chars()
        .filter(|c| !c.is_control())
        .take(MAX_VALUE)
        .collect();
    (!cleaned.is_empty()).then_some(cleaned)
}

pub fn parse_transport_info(xml: &[u8]) -> Result<TransportInfo, CastError> {
    let doc = document(xml)?;
    Ok(TransportInfo {
        state: value(&doc, "CurrentTransportState")
            .ok_or(CastError::Malformed("no CurrentTransportState"))?,
        status: value(&doc, "CurrentTransportStatus"),
        speed: value(&doc, "CurrentSpeed"),
    })
}

/// The UPnP error in a fault body, if the body parses at all.
pub fn parse_fault(xml: &[u8]) -> Option<Fault> {
    let doc = document(xml).ok()?;
    doc.descendants()
        .find(|node| node.is_element() && node.tag_name().name() == "Fault")?;
    Some(Fault {
        code: value(&doc, "errorCode").and_then(|code| code.parse().ok()),
        description: value(&doc, "errorDescription"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const AVT: &str = "urn:schemas-upnp-org:service:AVTransport:1";
    const URL: &str = "http://192.168.1.2:6878/cast/s/00112233445566778899aabbccddeeff.ts";

    fn vector(name: &str) -> Vec<u8> {
        let path = format!(
            "{}/../../tests/vectors/dlna/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read(path).unwrap()
    }

    #[test]
    fn set_av_transport_uri_matches_the_fixture() {
        let built = set_av_transport_uri(AVT, URL, "Outpace <live> & co", TS_MIME_TYPES[0]);
        assert!(set_av_transport_uri(AVT, URL, "t", TS_MIME_TYPES[1])
            .contains("protocolInfo=&quot;http-get:*:video/mpeg:*&quot;"));
        let expected = String::from_utf8(vector("soap-set-av-transport-uri.xml")).unwrap();
        assert_eq!(built, expected.trim_end());
        // The metadata is a nested, escaped document that parses back to DIDL-Lite.
        let doc = roxmltree::Document::parse(&built).unwrap();
        let metadata = doc
            .descendants()
            .find(|n| n.has_tag_name("CurrentURIMetaData"))
            .unwrap()
            .text()
            .unwrap();
        let didl = roxmltree::Document::parse(metadata).unwrap();
        let res = didl
            .descendants()
            .find(|n| n.tag_name().name() == "res")
            .unwrap();
        assert_eq!(
            res.attribute("protocolInfo"),
            Some("http-get:*:video/mp2t:*")
        );
        assert_eq!(res.text(), Some(URL));
        let title = didl
            .descendants()
            .find(|n| n.tag_name().name() == "title")
            .unwrap();
        assert_eq!(title.text(), Some("Outpace <live> & co"));
    }

    #[test]
    fn simple_actions_are_well_formed() {
        for (body, action) in [
            (play(AVT), "Play"),
            (stop(AVT), "Stop"),
            (get_transport_info(AVT), "GetTransportInfo"),
        ] {
            let doc = roxmltree::Document::parse(&body).unwrap();
            let node = doc
                .descendants()
                .find(|n| n.tag_name().name() == action)
                .unwrap();
            assert_eq!(node.tag_name().namespace(), Some(AVT));
            assert_eq!(
                node.children()
                    .find(|n| n.has_tag_name("InstanceID"))
                    .and_then(|n| n.text()),
                Some("0")
            );
        }
        assert!(play(AVT).contains("<Speed>1</Speed>"));
        assert_eq!(
            soap_action(AVT, "Play"),
            "\"urn:schemas-upnp-org:service:AVTransport:1#Play\""
        );
    }

    #[test]
    fn parses_transport_info() {
        let info = parse_transport_info(&vector("soap-get-transport-info-response.xml")).unwrap();
        assert_eq!(
            info,
            TransportInfo {
                state: "PLAYING".into(),
                status: Some("OK".into()),
                speed: Some("1".into()),
            }
        );
        assert!(parse_transport_info(b"<a/>").is_err());
        assert_eq!(
            parse_transport_info(&vec![b' '; MAX_SOAP_RESPONSE + 1]),
            Err(CastError::TooLarge)
        );
    }

    #[test]
    fn parses_upnp_faults() {
        assert_eq!(
            parse_fault(&vector("soap-fault.xml")),
            Some(Fault {
                code: Some(714),
                description: Some("Illegal MIME-type".into()),
            })
        );
        assert_eq!(parse_fault(b"<html>error</html>"), None);
        assert_eq!(parse_fault(b"not xml"), None);
    }

    #[test]
    fn escape_strips_control_characters() {
        assert_eq!(escape("a\u{0}b\u{1b}c'\""), "abc&apos;&quot;");
    }
}
