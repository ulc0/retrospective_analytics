from .._internal._runtime import RuntimeEnvironmentType, _assert_foundry_env, _get_runtime_env


class _ModelVersionRepr:
    """A class for handling model version html repr after model version publish in CWS"""

    def __init__(self, model_rid: str, model_version_rid: str):
        self.model_rid = model_rid
        self.model_version_rid = model_version_rid
        self.runtime_env = _get_runtime_env()

    def _proxy_url(self):
        foundry_env_url = _assert_foundry_env("FOUNDRY_EXTERNAL_HOST")
        return foundry_env_url

    def _repr_html_(self):
        # html repr only works when running in code workspaces jupyter environment. The util used by container xforms
        # to run notebook as a script still calls html repr, and will look for the "FOUNDRY_EXTERNAL_HOST" env var
        # which doesn't exist in container transforms
        if self.runtime_env.get_runtime_environment_type() is not RuntimeEnvironmentType.CODE_WORKSPACES:
            return ""

        iframe_url = (
            f"{self._proxy_url()}/workspace/code-workspaces/"
            f"iframes/model-version-preview?modelRid={self.model_rid}&"
            f"modelVersionRid={self.model_version_rid}&embedded=true"
        )
        # width is set to 100% - 2px to show the full width without a
        # horizontal scrollbar (borders take up horizontal space)
        return f"""
                <iframe
                    id="adapter"
                    src="{iframe_url}"
                    style="height: 300px; width: calc(100% - 2px); border: 1px solid #D3D8DE; border-radius: 5px;"
                    sandbox="allow-same-origin allow-scripts allow-popups"
                />
            """
