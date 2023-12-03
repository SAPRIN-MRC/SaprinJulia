using CSV
using UUIDs
using DataFrames
using NearestNeighbors #removed from packages - put back if you need to use this
using StaticArrays

function docluster!(df, kdtree, clustersize)
    c = 1
    nextidx = 1
    remaining = nrow(df)-1
    while remaining >= clustersize
        df[nextidx, :cluster] = c
        idxs, dists = knn(kdtree,df[nextidx, :coordinate], clustersize, true, n -> df[n, :cluster] > 0)
        for i = 1 : length(idxs)-1
            df[idxs[i],:cluster] = c
        end
        c = c + 1
        nextidx = idxs[end] #furthest point is start of next cluster
        remaining = remaining - clustersize
    end
end

df = CSV.File("C:\\Users\\kobus\\.julia\\dev\\SAPRINCore\\src\\OccupiedBS.csv",types=[Int,Int,Char,Float64,Float64,UUID]) |> DataFrame
transform!(df, [:Longitude,:Latitude] => ByRow((x,y) -> SVector{2}(x,y)) => :coordinate)
#filter!(r -> r.IntID <= 30, df)
insertcols!(df, :cluster => 0)

kdtree = KDTree(df.coordinate)

docluster!(df, kdtree, 5)

select!(df, Not([:coordinate]))

CSV.write("C:\\Users\\kobus\\.julia\\dev\\SAPRINCore\\src\\ClusterBS.csv", df)