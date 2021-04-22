using CSV
using UUIDs
using DataFrames
using NearestNeighbors
using StaticArrays

function enumeratetree(tree, index_range)
    df = DataFrame(cluster = Int64[], longitude = Float64[], latitude = Float64[])
    sf = DataFrame()
    for (i, idx) = enumerate(index_range)
        range = NearestNeighbors.get_leaf_range(tree.tree_data, idx)
        d = tree.data[range]
        longs = getindex.(d,1)
        lats = getindex.(d,2)
        #sf = DataFrame(cluster = fill(i,(length(d),1)), longitude = longs[:,:], latitude = lats[:,:])
        sf = DataFrame(longitude = longs, latitude = lats)
        insertcols!(sf, :cluster => i)
        append!(df,sf)
    end
    return df
end
df = CSV.File("C:\\Users\\kobus\\.julia\\dev\\SAPRINCore\\src\\OccupiedBS.csv",types=[Int,Int,Char,Float64,Float64,UUID]) |> DataFrame
transform!(df, [:Longitude,:Latitude] => ByRow((x,y) -> SVector{2}(x,y)) => :coordinate)
#filter!(r -> r.IntID <= 30, df)
insertcols!(df, :cluster => 0)

balltree = BallTree(df.coordinate,Euclidean(); leafsize = 10)

#skip non leaf nodes
offset = balltree.tree_data.n_internal_nodes + 1
nleafs = balltree.tree_data.n_leafs
index_range = offset: offset + nleafs - 1

df2 = enumeratetree(balltree, index_range)

CSV.write("C:\\Users\\kobus\\.julia\\dev\\SAPRINCore\\src\\ClusterBS2.csv", df2)