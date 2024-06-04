from localstack.runtime import hooks

_applied = False


def apply_patches():
    global _applied
    if _applied:
        return
    _applied = True

    # patches related to moto
    from localstack.services.infra import patch_instance_tracker_meta
    from localstack.utils.aws.request_context import patch_moto_request_handling

    patch_moto_request_handling()
    patch_instance_tracker_meta()


@hooks.on_infra_start(priority=100)
def _apply_aws_patches():
    apply_patches()
