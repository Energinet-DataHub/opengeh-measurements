class ElectricalHeatingFixture:
    def __init__(self, fixture):
        self._fixture = fixture

    def test_bar(self):
        pass


class TestElectricalHeating(ElectricalHeatingFixture):
    def __init__(self):
        super.__init__(derivedFixture)
