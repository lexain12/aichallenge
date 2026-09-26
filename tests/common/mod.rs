pub fn private_tempdir() -> tempfile::TempDir {
    let temporary_root = std::fs::canonicalize(std::env::temp_dir())
        .expect("resolve the platform temporary directory for nofollow tests");
    let directory = tempfile::tempdir_in(temporary_root).expect("create temporary directory");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
            .expect("make temporary directory owner-only");
    }
    directory
}
