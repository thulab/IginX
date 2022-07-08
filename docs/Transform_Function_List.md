# Python Transform Functions

## List of completed functions

* SPREAD()
* MEAN()
* BOTTOM()
* DISTINCT()

## List of functions to be completed

* STDDEV()
* AGGREGATE.RATE()
* AGGREGATE.WINDOW()
* MAP
* MEAN
* MEDIAN
* MODE
* highestAVERAGE()
* highestCURRENT()
* highestMAX()
* lowestAVERAGE()
* lowestCurrent()
* hourSELECTION()
* movingAVERAGE()
* RANGE()
* SKEW()
* SORT()
* SPREAD()
* ARRAY.FILTER
* INTEGRAL()
* LIMIT()
* REDUCE()
* SUM()
* tableFIND

## Function Definitions and Explanation

* BOTTOM() function
> The bottom() function sorts a table by columns and keeps only the bottom n records. bottom() is a selector function.
>
> bottom() 函数按列对表进行排序并仅保留底部的 n 条记录。 bottom() 是一个选择器函数。

* DISTINCT() function
>  Returns the unique field values associated with the field key. 
> DISTINCT() supports all field value data types. InfluxQL supports nesting DISTINCT() with COUNT().
>
> 返回与字段键关联的唯一字段值。
> DISTINCT() 支持所有字段值数据类型。 InfluxQL 支持将 DISTINCT() 与 COUNT() 嵌套。

* SPREAD()
> The spread() function outputs the difference between the minimum and maximum values in a specified column. spread() is an aggregate function. 
> 
> spread() 函数输出指定列中最小值和最大值之间的差。 spread() 是一个聚合函数。

* STDDEV()
> The stddev() function computes the standard deviation of non-null records in a specified column. stddev() is an aggregate function.
> 
> stddev() 函数计算指定列中非空记录的标准差。 stddev() 是一个聚合函数。

* aggregate.rate()
> The aggregate.rate() function calculates the rate of change per windows of time for each input table.
> The aggregate.rate() function calculates the rate of change per windows of time for each input table.
> The strategy of this function is to split data into pieces (into months), apply some function to each piece (sum up the amount of rainfall), combine the results back together again (plot the data or store the data for further usage).
> Example: If you want to measure the mean diurnal rainfall in Berlin from 2002 to 2016.
> 
> aggregate.rate() 函数计算每个输入表的每个时间窗口的变化率。

* aggregateWindow()
> aggregateWindow() applies an aggregate or selector function (any function with a column parameter) to fixed windows of time.
> 
> aggregateWindow() 将聚合或选择器函数（任何具有列参数的函数）应用于固定的时间窗口。

* highestAverage
> The highestAverage() function calculates the average of each table in the input stream returns the top n records. The function outputs a single aggregated table containing n records. highestAverage() is a selector function.
>
> highestAverage() 函数计算输入流中每个表的平均值，返回前 n 条记录。 该函数输出一个包含 n 条记录的聚合表。 highestAverage() 是一个选择器函数。

* highestCurrent
> The highestCurrent() function selects the last record of each table in the input stream and returns the top n records. The function outputs a single aggregated table containing n records. highestCurrent() is a selector function.
>
> highestCurrent() 函数选择输入流中每个表的最后一条记录，并返回前 n 条记录。 该函数输出一个包含 n 条记录的聚合表。 highcurrent() 是一个选择器函数。

* highestMax()
> The highestMax() function selects the maximum record from each table in the input stream and returns the top n records. The function outputs a single aggregated table containing n records. highestMax() is a selector function.
> 
> highestMax() 函数从输入流中的每个表中选择最大记录并返回前 n 条记录。 该函数输出一个包含 n 条记录的聚合表。 highMax() 是一个选择器函数。

* lowestAverage()
> The lowestAverage() function calculates the average of each table in the input stream returns the lowest n records. The function outputs a single aggregated table containing n records. lowestAverage() is a selector function.
>
> minimumAverage() 函数计算输入流中每个表的平均值，返回最低的 n 条记录。 该函数输出一个包含 n 条记录的聚合表。 minimumAverage() 是一个选择器函数。

* lowestCurrent() function
> The lowestCurrent() function selects the last record of each table in the input stream and returns the lowest n records. The function outputs a single aggregated table containing n records. lowestCurrent() is a selector function.
> 
> minimumCurrent() 函数选择输入流中每个表的最后一条记录，并返回最低的 n 条记录。 该函数输出一个包含 n 条记录的聚合表。 最低电流（）是一个选择器函数。

* hourSelection() function
> The hourSelection() function retains all rows with time values in a specified hour range.
>
> hourSelection() 函数保留时间值在指定小时范围内的所有行。

* movingAverage
> The movingAverage() function calculates the mean of values in the _values column grouped into n number of points.
>
> movingAverage() 函数计算 values 列中的值的平均值，这些值分组为 n 个点。

* range()
> The range() function filters records based on time bounds. Each input table’s records are filtered to contain only records that exist within the time bounds. Records with a null value for their time are filtered. Each input table’s group key value is modified to fit within the time bounds. Tables where all records exists outside the time bounds are filtered entirely.
>
> range() 函数根据时间限制过滤记录。 每个输入表的记录都被过滤为仅包含在时间范围内存在的记录。 过滤时间为空值的记录。 每个输入表的组键值都被修改以适应时间范围。 所有记录都存在于时间范围之外的表将被完全过滤。

* skew()
> The skew() function outputs the skew of non-null records as a float. skew() is an aggregate function.
> Output data type: Float
>
> skew() 函数将非空记录的倾斜输出为浮点数。 skew() 是一个聚合函数。
> 输出数据类型：浮点数

* sort()
> The sort() function orders the records within each table. One output table is produced for each input table. The output tables will have the same schema as their corresponding input tables.
>
> sort() 函数对每个表中的记录进行排序。 为每个输入表生成一个输出表。 输出表将具有与其对应的输入表相同的模式。

* spread()
> The spread() function outputs the difference between the minimum and maximum values in a specified column. spread() is an aggregate function.
>
> spread() 函数输出指定列中最小值和最大值之间的差。 spread() 是一个聚合函数。

* array.filter()
> array.filter() iterates over an array, evaluates each element with a predicate function, and then returns a new array with only elements that match the predicate.
>
> array.filter() 遍历一个数组，使用谓词函数评估每个元素，然后返回一个新数组，其中仅包含与谓词匹配的元素。

* integral() function
> The integral() function computes the area under the curve per unit of time of subsequent non-null records. integral() requires start and stop columns that are part of the group key. The curve is defined using time as the domain and record values as the range. 
> integral() is an aggregate function.
> 
> 积分() 函数计算后续非空记录的每单位时间曲线下面积。 integer() 需要 start 和 stop 列，它们是组键的一部分。 使用 time 作为域和记录值作为范围来定义曲线。
> 积分（）是一个聚合函数。

* limit() function
> The limit() function limits each output table to the first n records. The function produces one output table for each input table. 
> Each output table contains the first n records after the offset. If the input table has less than offset + n records, limit() outputs all records after the offset. 
> limit() is a selector function.
>
> limit() 函数将每个输出表限制为前 n 条记录。 该函数为每个输入表生成一个输出表。 每个输出表都包含偏移量之后的前 n 条记录。 如果输入表的记录数少于 offset + n 条，
> limit() 会输出偏移量之后的所有记录。 
> limit() 是一个选择器函数。

* reduce() function
> The reduce() function aggregates records in each table according to the reducer, fn, providing a way to create custom aggregations. The output for each table is the group key of the table with columns corresponding to each field in the reducer record. reduce() is an aggregate function.
>
> reduce() 函数根据 reducer fn 聚合每个表中的记录，提供了一种创建自定义聚合的方法。 每个表的输出是表的组键，其列对应于 reducer 记录中的每个字段。
> reduce() 是一个聚合函数。

* sum() function
> The sum() function computes the sum of non-null records in a specified column. sum() is an aggregate function.
>
> sum() 函数计算指定列中非空记录的总和。 sum() 是一个聚合函数。
