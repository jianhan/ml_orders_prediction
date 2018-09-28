import pandas as pd

def run():
    read_csv()

def read_csv():
    # Read CSV
    df = pd.read_csv('orderskus.csv')
    print("Size: ", df.size)
    print("Number of rows::",df.shape[0])
    print("Number of columns::",df.shape[1] )
    print("Column Names::",df.columns.values.tolist())
    print("Column Data Types::\n",df.dtypes)
    print("Columns with Missing Values::",df.columns[df.isnull().any()].tolist())
    print("Number of rows with Missing Values::",len(pd.isnull(df).any(1).nonzero()[0].tolist())) 
    print("Sample Indices with missing data::",pd.isnull(df).any(1).nonzero()[0].tolist()[0:5] )
    print("General Stats::")
    print(df.info())
    print("Summary Stats::" )
    print(df.describe())

class OrdersPrediction:
    """A simple class will implements the process of orders prediction according to date, delivery zone, order items."""

    def __init__(self, csv):
        """Constructor contains csv path"""
        try:
            self.csv = csv
            self.df = pd.read_csv(csv)
        except IOError:
            print('error occur while initialize OrdersPrediction: IOError')
        except:
            print('error occur while initialize OrdersPrediction')

    def __dataCollection(self):
       pass

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
        pass

   

# start ML pipeline
OrdersPrediction("orderskus.csv").startPipeLine()