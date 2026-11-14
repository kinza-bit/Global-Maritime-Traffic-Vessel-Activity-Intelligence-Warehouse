def container_path(cfg, container, *parts):
    base = f"abfss://{cfg['containers'][container]}@{cfg['storage_account']}.dfs.core.windows.net"
    return "/".join([base] + [p.strip("/") for p in parts])
