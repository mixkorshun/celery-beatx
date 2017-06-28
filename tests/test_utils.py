import pytest

from beatx.utils import import_string


class TestImportString:
    def test_import_module(self):
        mod = import_string('os.path')

        assert hasattr(mod, 'abspath')

    def test_import_function(self):
        pow = import_string('math.pow')
        assert pow(2, 3) == 8

    def test_import_non_existent(self):
        with pytest.raises(ImportError):
            import_string('os.non_existent')

    def test_import_non_module(self):
        with pytest.raises(ImportError):
            import_string('os')
