//! `tensorlake://` addresses — this worker owns the syntax past the scheme.

/// A parsed `tensorlake://` address.
///
/// `tensorlake://[account[/image[/process]]]`, every part optional and every
/// absent part filled in from configuration.
///
/// * **account** — which credentials and endpoints to use. Not a Tensorlake
///   concept: the API authenticates with a key and the key implies the
///   project, so this names a profile under `[transports.tensorlake.accounts]`.
///   That is also what makes a self-hosted endpoint a config entry rather than
///   a code change.
/// * **image** — the sandbox image. Empty means Tensorlake's default
///   environment.
/// * **process** — the executable to start inside the sandbox. This is the
///   SDK's entry point, not a script: it acquires the task and settles the
///   promise itself.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TensorlakeAddress {
    pub account: Option<String>,
    pub image: Option<String>,
    pub process: Option<String>,
}

impl TensorlakeAddress {
    /// Parse a `tensorlake://` address, or say why it is not one.
    ///
    /// The path is positional — first segment image, everything after it
    /// process — which is ambiguous for the image names that contain a slash
    /// (`tensorlake/ubuntu-minimal` is one). `?image=` and `?process=` override
    /// the positional reading and are the way to name those; when either is
    /// given it wins outright, so the two forms never have to be reconciled.
    pub fn parse(address: &str) -> Result<Self, String> {
        let parsed = url::Url::parse(address).map_err(|e| format!("invalid address: {e}"))?;
        if parsed.scheme() != super::SCHEME {
            return Err(format!(
                "expected {}:// scheme, got {}://",
                super::SCHEME,
                parsed.scheme()
            ));
        }

        let account = non_empty(parsed.host_str().unwrap_or(""));

        let mut image = None;
        let mut process = None;
        for (k, v) in parsed.query_pairs() {
            match k.as_ref() {
                "image" => image = non_empty(&v),
                "process" => process = non_empty(&v),
                other => return Err(format!("unknown query parameter: {other}")),
            }
        }

        // Positional fallback, per part: `?process=` alone still leaves the
        // whole path meaning the image, because a path that was going to be
        // split has no second reading once the process is named outright.
        let path = parsed.path().trim_start_matches('/');
        if !path.is_empty() {
            match (image.is_none(), process.is_none()) {
                (true, true) => match path.split_once('/') {
                    Some((first, rest)) => {
                        image = non_empty(first);
                        process = non_empty(rest);
                    }
                    None => image = non_empty(path),
                },
                (true, false) => image = non_empty(path),
                (false, true) => process = non_empty(path),
                (false, false) => {
                    return Err(
                        "path is meaningless when both ?image= and ?process= are given".to_string(),
                    )
                }
            }
        }

        Ok(Self {
            account,
            image,
            process,
        })
    }
}

fn non_empty(s: &str) -> Option<String> {
    let s = s.trim_matches('/');
    (!s.is_empty()).then(|| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(a: &str) -> TensorlakeAddress {
        TensorlakeAddress::parse(a).unwrap()
    }

    #[test]
    fn bare_address_defers_everything_to_config() {
        assert_eq!(parse("tensorlake://"), TensorlakeAddress::default());
    }

    #[test]
    fn account_only() {
        assert_eq!(parse("tensorlake://prod").account.as_deref(), Some("prod"));
    }

    #[test]
    fn account_and_image() {
        let a = parse("tensorlake://prod/python-3.11");
        assert_eq!(a.account.as_deref(), Some("prod"));
        assert_eq!(a.image.as_deref(), Some("python-3.11"));
        assert_eq!(a.process, None);
    }

    #[test]
    fn account_image_and_process() {
        let a = parse("tensorlake://prod/python-3.11/usr/bin/worker");
        assert_eq!(a.account.as_deref(), Some("prod"));
        assert_eq!(a.image.as_deref(), Some("python-3.11"));
        assert_eq!(a.process.as_deref(), Some("usr/bin/worker"));
    }

    #[test]
    fn image_without_account() {
        // The account slot is empty, not absent: `tensorlake:///image`.
        let a = parse("tensorlake:///python-3.11");
        assert_eq!(a.account, None);
        assert_eq!(a.image.as_deref(), Some("python-3.11"));
    }

    #[test]
    fn query_image_takes_the_whole_path_as_process() {
        // The escape hatch for an image name with a slash in it.
        let a = parse("tensorlake://prod/opt/worker?image=tensorlake/ubuntu-minimal");
        assert_eq!(a.image.as_deref(), Some("tensorlake/ubuntu-minimal"));
        assert_eq!(a.process.as_deref(), Some("opt/worker"));
    }

    #[test]
    fn query_process_takes_the_whole_path_as_image() {
        let a = parse("tensorlake://prod/tensorlake/ubuntu-minimal?process=/opt/worker");
        assert_eq!(a.image.as_deref(), Some("tensorlake/ubuntu-minimal"));
        assert_eq!(a.process.as_deref(), Some("opt/worker"));
    }

    #[test]
    fn both_queries_leave_the_path_with_no_meaning() {
        assert!(TensorlakeAddress::parse("tensorlake://p/x?image=i&process=p").is_err());
    }

    #[test]
    fn unknown_query_parameter_is_rejected() {
        assert!(TensorlakeAddress::parse("tensorlake://p?imagr=typo").is_err());
    }

    #[test]
    fn wrong_scheme_is_rejected() {
        assert!(TensorlakeAddress::parse("bash://tensorlake/x").is_err());
    }
}
