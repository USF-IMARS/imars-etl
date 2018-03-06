from datetime import datetime

class BaseSatFilepath():
    """
    Base constructor for saffilename pathing function.
    Instantiate with the filename format string and product_id.
    Calls to instantiations of this class will return the path.
    """
    def __init__(self, product_id, filename_fmt):
        """
            Parameters
            --------------
            filename_fmt : str
                Filename format string.
                Supports formatters from datetime.strftime.
                Example: "%Y-%m-%dT%H:%M:%SZ_my_product.png"
            product_id : str
                Unique string to identify the product.
                Determines which subdir to use within imars-objects/region/.
        """
        self.filename_fmt = filename_fmt
        self.product_id=product_id

    def basepath(self, region):
        """ returns the directory these products are placed in. """
        return "/srv/imars-objects/modis_aqua_{}/{}/".format(region, self.product_id)

    def match_filename(self, test_string):
        """
        returns true if given test_string could be a valid filename for this
        product. Else returns false.
        """
        try:
            datetime.strptime(test_string, self.filename_fmt)
            # TODO: other tests for filename validity?
            return True
        except:
            return False

    # TODO: def match_path()
    # TODO: def match() to dynamically choose match_path or match_filename

    def __call__(self, exec_time, region_id):
        """
        Parameters
        ------------
        exec_time : datetime.datetime
            execution_date for the product (eg the start_time of the granule)
        region_id : str
            region.place_name from regions.<region_name> used to create path
            within imars-objects.
        """
        return self.basepath(region_id) + exec_time.strftime(self.filename_fmt)
