This use case requirement is as follows:
 

**********************************************************************
DATA

	We have an input file (recipes.json) containing details of different recipes.
	The schema looks like:
	  root
	   |-- cookTime: string (nullable = true)
	   |-- datePublished: string (nullable = true)
	   |-- description: string (nullable = true)
	   |-- image: string (nullable = true)
	   |-- ingredients: string (nullable = true)
	   |-- name: string (nullable = true)
	   |-- prepTime: string (nullable = true)
	   |-- recipeYield: string (nullable = true)
	   |-- url: string (nullable = true) 


If total time taken to get a dish ready is the sum of prepTime and cookTime.
We categorise the difficulty level of a dish on the basis of totalTime.

  easy   : if total time taken is less than 30 mins
  medium : if it is not easy and total time taken is less than 60 mins
  hard   : if it is not medium and total time taken is more than 60 mins

PROBLEM STATEMENT

	Read the data file and find out the average of the total time taken to prepare all the dishes for each difficulty level.

CONSIDERATIONS

	cookTime and prepTime are in ISO format
	Prepare the project as a python project
	Create a data directory year/month/day inside the project resources
	Place input data (recipes.json) in the data directory and read from the data directory
  ******************************************************************************************************
  
  -------------------->Used PySpark  Dataframes concept to solve this usecase.
