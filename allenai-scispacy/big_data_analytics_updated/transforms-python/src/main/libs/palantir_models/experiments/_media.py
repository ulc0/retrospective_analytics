import io
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    try:
        import PIL
    except ModuleNotFoundError:
        pass

PUT_IMAGE_UNION = Union[str, bytes, "PIL.Image.Image"]


def assert_is_png(data: bytes) -> bytes:
    png_signature = b"\x89PNG\r\n\x1a\n"
    assert data.startswith(png_signature), "Expected png image"
    return data


def read_and_validate_image(image: PUT_IMAGE_UNION) -> bytes:
    if isinstance(image, bytes):
        return assert_is_png(image)
    if isinstance(image, str):
        with open(image, "rb") as f:
            return assert_is_png(f.read())

    try:
        from PIL import Image

        if isinstance(image, Image.Image):
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format="PNG")
            return img_byte_arr.getvalue()
    except ModuleNotFoundError:
        pass

    raise TypeError(f'Unexpected image type "{type(image)}"')
