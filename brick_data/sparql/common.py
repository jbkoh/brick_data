import semver

# CONSTANTS

VIRTUOSO = 'virtuoso'


def semver_compare(src_version, target_version):
    src_version = _append_patch_ver(src_version)
    target_version = _append_patch_ver(target_version)
    return semver.compare(src_version, target_version)
