import re

conversions = {
    "M": 1e6,
    "G": 1e9,
    "Mi": 2**20,
    "Gi": 2**30,
}


def parse_human_readable_bytes(value: str) -> int:
    matches = re.match(r"^(\d+\.?\d*)([MG]i?)$", value)
    if matches is None:
        raise Exception(
            "Invalid human readable byte quantity. Value must be [number][M/G/Mi/Gi]. For example: 1G, or 500M."
        )

    amount = matches.group(1)
    format = matches.group(2)

    if format not in conversions:
        raise Exception(f'Unsupported format "{format}". Value must be [number][M/G/Mi/Gi]. For example: 1G, or 500M.')

    return int(float(amount) * conversions[format])
