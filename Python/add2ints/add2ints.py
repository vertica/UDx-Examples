import vertica_sdk

class add2ints(vertica_sdk.ScalarFunctionFactory):
    """Return the sum of two integer columns"""

    def __init__(self):
        pass
    def setup(self, server_interface, col_types):
        pass
    def processBlock(self, server_interface, arg_reader, res_writer):
        # Writes a string to the UDx log file.
        server_interface.log("Python UDx - Adding 2 ints!")
        while(True):
            # Example of error checking best practices.
            product_id = arg_reader.getInt(2)
            if product_id < 100:
                raise ValueError("Invalid Product ID")
            if arg_reader.isNull(0) or arg_reader.isNull(1):
                raise ValueError("I found a NULL!")
            else:
                first_int = arg_reader.getInt(0) # Input column
                second_int = arg_reader.getInt(1) # Input column
            res_writer.setInt(first_int + second_int) # Sum of input columns.
            res_writer.next() # Read the next row.
            if not arg_reader.next():
                # Stop processing when there are no more input rows.
                break
    def destroy(self, server_interface, col_types):
        pass

class add2ints_factory(vertica_sdk.ScalarFunctionFactory):
    
    def createScalarFunction(self, srv):
        return add2ints()

    def getPrototype(self, srv_interface, arg_types, return_type):
        arg_types.addInt()
        arg_types.addInt()
        arg_types.addInt()
        return_type.addInt()

    def getReturnType(self, srv_interface, arg_types, return_type):
        return_type.addInt()
