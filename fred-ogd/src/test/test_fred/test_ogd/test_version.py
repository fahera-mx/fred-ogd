from fred.ogd.version import version


def test_version():
    assert isinstance(version.value, str)
    assert len(version.components()) == 3
    assert all(isinstance(comp, str) for comp in version.components())
    assert all(isinstance(comp, int) for comp in version.components(as_int=True))
    assert version.upcoming(major=True).major == version.major + 1
    assert version.upcoming(minor=True).minor == version.minor + 1
    assert version.upcoming(patch=True).patch == version.patch + 1
