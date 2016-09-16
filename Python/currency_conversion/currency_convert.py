import vertica_sdk
import decimal

rates2USD = {'USD': 1.000,
             'EUR': 0.89977,
             'GBP': 0.68452,
             'INR': 67.0345,
             'AUD': 1.39187,
             'CAD': 1.30335,
             'ZAR': 15.7181}

class currency_convert(vertica_sdk.ScalarFunction):
    """Converts a money column to another currency
    
    Returns a value in USD.
    
    """
    def __init__(self):
        pass

    def setup(self, server_interface, col_types):
        pass

    def processBlock(self, server_interface, arg_reader, res_writer):
        server_interface.log(" $$$ ")

        while(True):
            currency = arg_reader.getString(0)
            try:
                rate = decimal.Decimal(rates2USD[currency])

            except KeyError:
                server_interface.log("ERROR: {} not in dictionary.".format(currency))
                # Scalar functions always need a value to move forward to the
                # next input row. Therefore, we need to assign it a value to
                # move beyond the error.
                currency = 'USD'
                rate = decimal.Decimal(rates2USD[currency])

            starting_value = arg_reader.getNumeric(1)
            converted_value = decimal.Decimal(starting_value  / rate)
            res_writer.setNumeric(converted_value)
            res_writer.next()
            if not arg_reader.next():
                break

    def destroy(self, server_interface, col_types):
        pass

class currency_convert_factory(vertica_sdk.ScalarFunctionFactory):

    def createScalarFunction(self, srv):
        return currency_convert()

    def getPrototype(self, srv_interface, arg_types, return_type):
        arg_types.addVarchar()
        arg_types.addNumeric()
        return_type.addNumeric()

    def getReturnType(self, srv_interface, arg_types, return_type):
        return_type.addNumeric(9,4)

