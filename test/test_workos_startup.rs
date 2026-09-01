/// Integration test: verify that a Config with a workos section
/// deserializes correctly from a TOML file.
///
/// This validates the shape of the config, not the loader.
/// Use `toml::from_str` directly to test deserialization without
/// touching the filesystem loader or env vars.
use resonate::config::Config;

#[test]
fn workos_config_loads_with_memory_storage() {
    let text = std::fs::read_to_string("test/workos_config.toml")
        .expect("workos_config.toml should exist");

    let config: Config = toml::from_str(&text).expect("workos config should parse as TOML");

    assert!(config.workos.is_some(), "workos field should be present");
    let w = config.workos.as_ref().unwrap();
    // api_key removed — clients send their own key
    // client_id removed — clients send their own
    assert_eq!(w.org_id.as_deref(), Some("org_abc"));
}
