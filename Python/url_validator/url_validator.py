import vertica_sdk
import urllib.request
import time

class validate_url(vertica_sdk.ScalarFunction):
    """Validates HTTP requests.
    
    Returns the status code of a webpage. Pages that cannot be accessed return
    "Failed to load page."

    """

    def __init__(self):
        pass

    def setup(self, server_interface, col_types):
        pass

    def processBlock(self, server_interface, arg_reader, res_writer):
        # Writes a string to the UDx log file.
        server_interface.log("Validating URL Accessibility - UDx")

        while(True):
            url = arg_reader.getString(0)
            try:
                status = urllib.request.urlopen(url).getcode()
                # Avoid overwhelming web servers -- be nice.
                time.sleep(2)
            except (ValueError, urllib.error.HTTPError, urllib.error.URLError):
                status = 'Failed to load page'
            res_writer.setString(str(status))
            res_writer.next()
            if not arg_reader.next():
                # Stop processing when there are no more input rows.
                break

    def destroy(self, server_interface, col_types):
        pass

class validate_url_factory(vertica_sdk.ScalarFunctionFactory):

    def createScalarFunction(self, srv):
        return test_url()

    def getPrototype(self, srv_interface, arg_types, return_type):
        arg_types.addVarchar()
        return_type.addChar()

    def getReturnType(self, srv_interface, arg_types, return_type):
        return_type.addChar(20)
