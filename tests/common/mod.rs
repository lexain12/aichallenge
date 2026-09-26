pub fn private_tempdir() -> tempfile::TempDir {
    let directory = tempfile::tempdir().expect("create temporary directory");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
            .expect("make temporary directory owner-only");
    }
    directory
}
