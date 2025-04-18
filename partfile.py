import io
from pathlib import Path

class PartFile:
    path: Path
    tmp_path: Path
    part_suffix: str
    mode: str
    def __init__(self, path: Path, mode: str='r', part_suffix: str='.part'):
        self.path = path
        self.tmp_path = self.path.parent / f'{self.path.name}{part_suffix}'
        self.part_suffix = part_suffix
        self.mode = mode
    def __enter__(self):
        self._open_handle = self.tmp_path.open(self.mode)
        return self._open_handle
    def __exit__(self, exc_type, exc, exc_tb):
        if exc is not None:
            print(exc)
        self._open_handle.close()
        if exc_type is None:
            if self.part_suffix:
                try:
                    self.tmp_path.rename(self.path)
                except Exception as e:
                    try:
                        self.tmp_path.unlink()
                        raise e from exc
                    except Exception as e2:
                        e.__cause__ = exc
                        raise e2 from e
                    
        else:
            try:
                self.tmp_path.unlink()
            except Exception as e:
                raise e from exc

