using DataFrames

x = DataFrame(id=["a","b","a","c","c","a","c","a","a","c"], b=[2,5,7,8,3,9,1,10,4,8], c=["one","two","three","four","five","six","seven","eight","nine","ten"])
show(x)
println()
#y = combine(groupby(x,:id), sdf -> sort(sdf,:b), :id => eachindex => :rank, nrow => :n)
y = transform!(groupby(sort(x, [:id, :b]), :id), :id => eachindex => :rank, nrow => :n)