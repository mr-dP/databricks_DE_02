# Databricks notebook source
# Here we define a "Cube" class where its constructor accepts one argument which is the edge of the cube
# And it has 2 methods to calculate the cubes volume and surface area

# COMMAND ----------

class Cube:
    def __init__(self, edge):
        self.edge = edge

    def get_volume(self):
        a = self.edge
        return a * a * a

    def get_surface_area(self):
        a = self.edge
        return 6 * a * a
