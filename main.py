import pandas as pd
import logging

class OrdersPrediction:
    """A simple class will implements the process of orders prediction according to date, delivery zone, order items."""

    def __init__(self, csv):
        """Constructor contains csv path"""
        self.logger = logging.getLogger('ml')
        try:
            self.csv = csv
            self.df = pd.read_csv(csv)
        except Exception as e:
            self.logger.error('error occur while initialize OrdersPrediction'+str(e))

    def __dataCollection(self):
        print("Size: ", self.df.size)
        print("Number of rows::",self.df.shape[0])
        print("Number of columns::",self.df.shape[1] )
        print("Column Names::", self.df.columns.values.tolist())
        print("Column Data Types::\n", self.df.dtypes)
        print("Columns with Missing Values::", self.df.columns[self.df.isnull().any()].tolist())
        print("Number of rows with Missing Values::", len(pd.isnull(self.df).any(1).nonzero()[0].tolist())) 
        print("Sample Indices with missing data::", pd.isnull(self.df).any(1).nonzero()[0].tolist()[0:5] )
        print("General Stats::")
        print(self.df.info())
        print("Summary Stats::" )
        print(self.df.describe())
        # self.__cleanup_column_names({'delivery_zone': 'dz'})

    def __dataDescription(self):
       pass

    def __dataWrangling(self):
       pass

    def __dataVisualization(self):
       pass

    def __featureEngineering(self):
       pass

    def __modelBuilding(self):
        pass

    def __modelDeployment(self):
        pass

    def startPipeLine(self):
        """Entry point of start machine learning process"""
        try:
            self.__dataCollection()
        except Exception as e:
            self.logger.error('error occur while running pipeline'+str(e))

    def __cleanup_column_names(self, rename_dict={},do_inplace=True):
        """This function renames columns of a pandas dataframe
            It converts column names to snake case if rename_dict is not passed.
        Args:
            rename_dict (dict): keys represent old column names and values point to newer ones
            do_inplace (bool): flag to update existing dataframe or return a new one
        Returns:
            pandas dataframe if do_inplace is set to False, None otherwise
        """
        if not rename_dict:
            return self.df.rename(columns={col: col.lower().replace(' ','_') for col in self.df.columns.values.tolist()}, inplace=do_inplace)
        else:
            return self.df.rename(columns=rename_dict,inplace=do_inplace)

# start ML pipeline
OrdersPrediction("orderskus.csv").startPipeLine()