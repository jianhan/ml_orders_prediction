import datetime
import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


class OrdersPrediction:
    """A simple class will implements the process of orders prediction according to date, delivery zone, order items."""

    def __init__(self, csv):
        """Constructor contains csv path"""
        self.logger = logging.getLogger('ml')
        try:
            self.csv = csv
            self.df = pd.read_csv(csv)
        except Exception as e:
            self.logger.error('error occur while initialize OrdersPrediction' + str(e))

    def __dataCollection(self):
        print("Size: ", self.df.size)
        print("Number of rows::", self.df.shape[0])
        print("Number of columns::", self.df.shape[1])
        print("Column Names::", self.df.columns.values.tolist())
        print("Column Data Types::\n", self.df.dtypes)
        print("Columns with Missing Values::", self.df.columns[self.df.isnull().any()].tolist())
        print("Number of rows with Missing Values::", len(pd.isnull(self.df).any(1).nonzero()[0].tolist()))
        print("Sample Indices with missing data::", pd.isnull(self.df).any(1).nonzero()[0].tolist()[0:5])
        print("General Stats::")
        print(self.df.info())
        print("Summary Stats::")
        print(self.df.describe())
        # self.__cleanup_column_names({'delivery_zone': 'dz'})

    def __dataWrangling(self):
        # trim all
        df_obj = self.df.select_dtypes(['object'])
        self.df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

        # filtering data remove all missing values
        self.df['delivery_date'] = pd.to_datetime(self.df.delivery_date, format='%Y-%m-%d', errors='coerce')
        print(self.df.dtypes)
        print("Number of rows::", self.df.shape[0])
        print("Drop Rows with missing dates::")
        self.df = self.df.dropna(subset=['delivery_date'])
        self.df = self.df.dropna(subset=['delivery_zone'])
        self.df = self.df.dropna(subset=['sku'])
        self.df = self.df.dropna(subset=['purchased'])
        print("Shape::", self.df.shape)
        print("Columns with Missing Values::", self.df.columns[self.df.isnull().any()].tolist())

        # handling categorical data, delivery zone
        self.df['encoded_delivery_zone'] = self.df.delivery_zone.map(array_to_dict(self.df.delivery_zone.unique()))

        # handling categorical data, skus
        self.df['encoded_sku'] = self.df.sku.map(array_to_dict(self.df.sku.unique()))

        # filter invalid date
        self.df = self.df[self.df['delivery_date'] <= datetime.datetime.now()]
        print(self.df.sort_values(by=['delivery_date']))

    def __dataVisualization(self):
        # print(self.df['purchased'][self.df['encoded_delivery_zone']==4].mean())
        print(self.df.groupby(['sku'])['purchased'].sum())
        print(self.df.groupby(['sku', 'purchased']).agg({'purchased': {'total_purchased': np.sum,
                                                                       'mean_price': np.mean,
                                                                       'variance_price': np.std,
                                                                       'count': np.count_nonzero},
                                                         'purchased': np.sum}))

        self.df[self.df.encoded_delivery_zone == 4].plot(x='delivery_date', y='purchased', style='blue')
        plt.title('Price Trends for Particular User')
        plt.show()

        self.df[self.df.encoded_delivery_zone == 12].plot(x='delivery_date', y='purchased', style='blue')
        plt.title('Price Trends for Particular User')
        plt.show()

        # x = np.linspace(0, 20, 100)
        # plt.plot(x, np.sin(x))
        # plt.show()

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
            self.__dataWrangling()
            # self.__dataVisualization()
        except Exception as e:
            self.logger.error('error occur while running pipeline' + str(e))

    def __cleanup_column_names(self, rename_dict={}, do_inplace=True):
        """This function renames columns of a pandas dataframe
            It converts column names to snake case if rename_dict is not passed.
        Args:
            rename_dict (dict): keys represent old column names and values point to newer ones
            do_inplace (bool): flag to update existing dataframe or return a new one
        Returns:
            pandas dataframe if do_inplace is set to False, None otherwise
        """
        if not rename_dict:
            return self.df.rename(
                columns={col: col.lower().replace(' ', '_') for col in self.df.columns.values.tolist()},
                inplace=do_inplace)
        else:
            return self.df.rename(columns=rename_dict, inplace=do_inplace)


def array_to_dict(arr):
    arr.sort()
    retVal = {}
    for i in range(len(arr)):
        retVal[arr[i]] = i
    return retVal


# start ML pipeline
OrdersPrediction("orderskus.csv").startPipeLine()
