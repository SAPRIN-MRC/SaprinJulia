using DataFrames

x = DataFrame(id=["a","b","a","c","c","a","c","a","a","c"], b=[2,5,7,8,3,9,1,10,4,6], c=["one","two","three","four","five","six","seven","eight","nine","ten"])
show(x)
y = combine(groupby(x,:id), sdf -> sort(sdf,:b), :id => eachindex => :rank, nrow => :n)
# y = axes(x,1)
# show(y)
fromId = 2
toId = 4
filter!([:rank] => r -> r < fromId || r > toId, y)