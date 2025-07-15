import os

from scispacy.version import VERSION


def main():
    s3_prefix = "https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.4/"
    model_names = [
        "en_core_sci_sm",
        "en_core_sci_md",
        "en_core_sci_lg",
        "en_core_sci_scibert",
        "en_ner_bc5cdr_md",
        "en_ner_craft_md",
        "en_ner_bionlp13cg_md",
        "en_ner_jnlpba_md",
    ]

    full_package_paths = [
        f"{s3_prefix}{model_name}-{VERSION}.tar.gz" for model_name in model_names
    ]

    for package_path in full_package_paths:
        os.system(f"pip install {package_path}")


if __name__ == "__main__":
    main()
