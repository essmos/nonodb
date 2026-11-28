//! Profile isolation checks for columnar collections.

use nonodb_core::vault::CollectionProfile;
use nonodb_core::Store;

#[test]
fn vault_and_columnar_profiles_cannot_be_downgraded_or_converted() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store
        .create_collection("vaulted", CollectionProfile::Vault)
        .unwrap();
    assert!(store
        .set_columnar_settings("vaulted", &nonodb_core::columnar::Settings::default())
        .is_err());
    assert_eq!(
        store.collection_profile("vaulted").unwrap(),
        Some(CollectionProfile::Vault)
    );

    store
        .create_columnar_collection("events", &nonodb_core::columnar::Settings::default())
        .unwrap();
    assert!(store
        .create_collection("events", CollectionProfile::Vault)
        .is_err());
    assert!(store
        .create_collection("events", CollectionProfile::Standard)
        .is_err());
    assert_eq!(
        store.collection_profile("events").unwrap(),
        Some(CollectionProfile::Columnar)
    );
    assert!(store.columnar_settings("events").unwrap().is_some());
}
