import hashlib
import io
from PIL import Image
class ImageParser:
    """Handles the heavy lifting of decoding images and extracting metadata."""
    @staticmethod
    def to_metadata_blob(content, s3_obj_meta, content_type):
        """Pure logic: Bytes in, Dict out."""
        try:
            img = Image.open(io.BytesIO(content))
            width, height = img.size

            return {
                "label": s3_obj_meta.get('label'),
                "url": s3_obj_meta.get('url'),
                "file_size_bytes": len(content),
                "content_type": content_type,
                "width": width,
                "height": height,
                "aspect_ratio": round(width / height, 2) if height > 0 else 0,
                "image_mode": img.mode,
                "is_corrupted": False,
                "md5": hashlib.md5(content).hexdigest()
            }
        except Exception as e:
            return {"is_corrupted": True, "error_msg": str(e), "width": 0, "height": 0}