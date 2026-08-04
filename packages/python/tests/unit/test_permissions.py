import stat
import sys
from pathlib import Path

import pytest

from semantic_layer import initialize
from semantic_layer.permissions import secure_owner_only, windows_acl_args


def test_windows_acl_uses_stable_broad_principal_sids_and_current_user() -> None:
    assert windows_acl_args(Path("C:/capture"), directory=True, identity="DOMAIN\\user") == [
        "C:/capture",
        "/inheritance:r",
        "/remove:g",
        "*S-1-1-0",
        "*S-1-5-11",
        "*S-1-5-32-545",
        "/grant:r",
        "DOMAIN\\user:(OI)(CI)F",
    ]
    calls: list[list[str]] = []

    class Result:
        returncode = 0
        stderr = ""

    def runner(args: list[str], **_: object) -> Result:
        calls.append(args)
        return Result()

    secure_owner_only(
        Path("C:/capture/manifest.json"),
        directory=False,
        platform="win32",
        identity="user",
        runner=runner,
    )
    assert calls == [
        [
            "icacls",
            "C:/capture/manifest.json",
            "/inheritance:r",
            "/remove:g",
            "*S-1-1-0",
            "*S-1-5-11",
            "*S-1-5-32-545",
            "/grant:r",
            "user:F",
        ]
    ]


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX mode assertion")
def test_sdk_secures_created_output_components_without_chmodding_existing_ancestor(
    tmp_path: Path,
) -> None:
    existing_ancestor = tmp_path / "workspace"
    existing_ancestor.mkdir()
    existing_ancestor.chmod(0o755)
    privacy_boundary = existing_ancestor / ".semantic-layer"
    output = privacy_boundary / "traces"

    capture = initialize(output=output, service_name="permission-boundary")
    closed = capture.shutdown()

    assert stat.S_IMODE(existing_ancestor.stat().st_mode) == 0o755
    assert stat.S_IMODE(privacy_boundary.stat().st_mode) == 0o700
    assert stat.S_IMODE(output.stat().st_mode) == 0o700
    artifact = Path(closed.artifact_path)
    assert stat.S_IMODE(artifact.stat().st_mode) == 0o700
    assert stat.S_IMODE((artifact / "trace.jsonl").stat().st_mode) == 0o600
    assert stat.S_IMODE((artifact / "manifest.json").stat().st_mode) == 0o600


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX mode assertion")
def test_sdk_does_not_chmod_preexisting_privacy_boundary(tmp_path: Path) -> None:
    privacy_boundary = tmp_path / ".semantic-layer"
    privacy_boundary.mkdir()
    privacy_boundary.chmod(0o750)
    output = privacy_boundary / "traces"

    capture = initialize(output=output, service_name="existing-permission-boundary")
    closed = capture.shutdown()

    assert stat.S_IMODE(privacy_boundary.stat().st_mode) == 0o750
    assert stat.S_IMODE(output.stat().st_mode) == 0o700
    assert stat.S_IMODE(Path(closed.artifact_path).stat().st_mode) == 0o700


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX symlink assertion")
def test_sdk_rejects_intermediate_output_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink path component is not allowed"):
        initialize(output=linked / "traces", service_name="symlink-output")


@pytest.mark.skipif(
    sys.platform != "darwin" or not Path("/var").is_symlink(),
    reason="macOS root alias assertion",
)
def test_sdk_allows_standard_macos_root_alias(tmp_path: Path) -> None:
    private_var = Path("/private/var")
    relative = tmp_path.resolve().relative_to(private_var)
    aliased_output = Path("/var") / relative / "alias-output"

    capture = initialize(output=aliased_output, service_name="macos-root-alias")
    closed = capture.shutdown()

    assert Path(closed.artifact_path).is_dir()
