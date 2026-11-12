use hostmylauncher::{Event, Instance, RemoteFile, SyncMode};
use serde_json::json;

#[test]
fn deserializes_instance_with_nested_and_extra() {
    let v = json!({
        "id": "abc", "launcher_id": "lns", "slug": "pack", "title": "Pack",
        "minecraft_version": "1.20.1", "server": "mc.example",
        "loader": { "type": "fabric", "build": "latest", "enabled": true },
        "locked": false, "hide": true,
        "users": { "whitelist": ["u1"], "noPremium": true },
        "java_version": 17, "suggested_ram": 4096, "downloads": 9,
        "icon": "i.png", "code": "SECRET", "created_at": 1750000000
    });
    let inst: Instance = serde_json::from_value(v).unwrap();
    assert_eq!(inst.id, "abc");
    assert_eq!(inst.launcher_id.as_deref(), Some("lns"));
    assert_eq!(
        inst.loader.as_ref().unwrap().kind.as_deref(),
        Some("fabric")
    );
    assert!(inst.loader.as_ref().unwrap().enabled);
    assert!(inst.users.as_ref().unwrap().no_premium);
    assert_eq!(inst.java_version, Some(17));
    assert_eq!(inst.suggested_ram, Some(4096));
    assert_eq!(inst.downloads, 9);
    assert_eq!(inst.hide, Some(true));
    // unmodeled fields are preserved in `extra`
    assert_eq!(
        inst.extra.get("code").and_then(|c| c.as_str()),
        Some("SECRET")
    );
}

#[test]
fn java_version_accepts_string() {
    let inst: Instance = serde_json::from_value(json!({
        "id": "x", "java_version": "21"
    }))
    .unwrap();
    assert_eq!(inst.java_version, Some(21));
}

#[test]
fn deserializes_remote_file_rich_shape() {
    let f: RemoteFile = serde_json::from_value(json!({
        "path": "mods/x.jar", "size": 12345,
        "hashes": { "sha1": "aaa", "sha512": "bbb" },
        "downloads": ["https://cdn.example/x.jar"]
    }))
    .unwrap();
    assert_eq!(f.path.as_deref(), Some("mods/x.jar"));
    assert_eq!(f.size, Some(12345));
    assert_eq!(f.hashes.as_ref().unwrap().sha512.as_deref(), Some("bbb"));
    assert_eq!(f.downloads.len(), 1);
}

#[test]
fn parses_live_event() {
    let ev = Event::parse(json!({
        "type": "instance.created",
        "payload": { "id": "i1", "slug": "pack" },
        "revision": 7, "ts": 1
    }));
    match ev {
        Event::InstanceCreated { id, slug, revision } => {
            assert_eq!(id, "i1");
            assert_eq!(slug.as_deref(), Some("pack"));
            assert_eq!(revision, 7);
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn parses_sync_replay_with_nested_events() {
    let ev = Event::parse(json!({
        "type": "sync", "mode": "replay", "revision": 5,
        "events": [
            { "type": "instance.updated", "payload": { "id": "a" }, "revision": 4 },
            { "type": "instance.deleted", "payload": { "id": "b" }, "revision": 5 }
        ]
    }));
    match ev {
        Event::Sync {
            mode,
            revision,
            events,
        } => {
            assert_eq!(mode, SyncMode::Replay);
            assert_eq!(revision, 5);
            assert_eq!(events.len(), 2);
            assert!(matches!(events[1], Event::InstanceDeleted { .. }));
        }
        other => panic!("unexpected: {other:?}"),
    }
}
