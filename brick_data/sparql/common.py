import semver

# CONSTANTS

VIRTUOSO = 'virtuoso'

def _append_patch_ver(version):
    if len(version.split(".")) < 3:
        version += ".0"
    return version


def semver_compare(src_version, target_version):
    src_version = _append_patch_ver(src_version)
    target_version = _append_patch_ver(target_version)
    return semver.compare(src_version, target_version)
