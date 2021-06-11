using DataFrames

df = DataFrame(a = [1,2,3,4,5], 
               b = [1,2,3,4,5], 
               c = [Int8(1), Int8(2), Int8(3), Int8(4), Int8(5)])
transform!(df, names(df, Int64) .=> ByRow(Int8), renamecols=false) 
subset!(df, :a => x -> x .> 9)
transform!(df, names(df, Int64) .=> ByRow(Int8), renamecols=false) 
