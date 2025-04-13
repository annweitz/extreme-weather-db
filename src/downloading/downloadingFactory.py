class DownloadingFactory:

    requestBuilders = {}
    sanitycheckFunctions = {}

    @classmethod
    def registerRequestBuilder(cls, variableName, function):
        cls.requestBuilders[variableName] = function

    @classmethod
    def getRequestBuilder(cls, variableName):
        function = cls.requestBuilders.get(variableName)
        if not function:
            raise ValueError(f"No request builder found for variable {variableName}")
        return function

    @classmethod
    def registerSanityChecker(cls, variableName, function):
        cls.sanitycheckFunctions[variableName] = function

    @classmethod
    def getSanityChecker(cls, variableName):
        function = cls.sanitycheckFunctions.get(variableName)
        if not function:
            raise ValueError(f"No sanity check function found for variable {variableName}")
        return function

# Register request builders and sanity checkers
from downloadTemperature import requestBuilderTemperature, sanityCheckTemperature
from downloadWindgust import requestBuilderWindgust, sanityCheckWindgust
from downloadWind import requestBuilderWind, sanityCheckWind
from downloadPrecipitation import requestBuilderPrecipitation, sanityCheckPrecipitation

grid = [0.25,0.25]

DownloadingFactory.registerRequestBuilder("precipitation", lambda year, month: requestBuilderPrecipitation(year, month, grid = grid))
DownloadingFactory.registerRequestBuilder("temperature", lambda year, month: requestBuilderTemperature(year, month, grid = grid))
DownloadingFactory.registerRequestBuilder("wind", lambda year, month: requestBuilderWind(year, month, grid = grid))
DownloadingFactory.registerRequestBuilder("windgust", lambda year, month: requestBuilderWindgust(year, month, grid = grid))


DownloadingFactory.registerSanityChecker("temperature", lambda path: sanityCheckTemperature(path))
DownloadingFactory.registerSanityChecker("precipitation", lambda path: sanityCheckPrecipitation(path))
DownloadingFactory.registerSanityChecker("wind", lambda path: sanityCheckWind(path))
DownloadingFactory.registerSanityChecker("windgust", lambda path: sanityCheckWindgust(path))