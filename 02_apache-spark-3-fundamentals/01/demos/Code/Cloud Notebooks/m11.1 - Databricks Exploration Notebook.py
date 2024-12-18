# Databricks notebook source
data = [
            [ 1, "Neha",  10000 ],
            [ 2, "Steve", 20000 ],
            [ 3, "Kari",  30000 ],
            [ 4, "Ivan",  40000 ],
            [ 5, "Mohit", 50000 ]
       ]

# COMMAND ----------

employeesDF = (
                    spark
                        .createDataFrame
                        (
                            data,                                   # Pass RDD or collection
                            "Id: long, Name: string, Salary: long"  
                            
                                              # Pass schema as array ["Id", "Name", "Salary"]
                        )
               )

employeesDF.show()

# COMMAND ----------


