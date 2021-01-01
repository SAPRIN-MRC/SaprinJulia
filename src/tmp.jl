using DataFrames
using Arrow
using Dates
using Statistics
#using ShiftedArrays
# read Residencies
function test()
    r = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data","Agincourt","Staging","IndividualResidencies.arrow")) |> DataFrame
    r = r[1:10,:]
    n = nrow(r)
    println("$(n) residence rows")
    select!(r, [:ResidenceId, :IndividualId, :LocationId, :StartDate,:StartType, :EndDate, :EndType, :ResidentIndex])
    insertcols!(r,:ResidentIndex, :GapStart => 0, :GapEnd => 0, :Gap => 0)
    s = similar(r,0)
    @time for row in eachrow(r)
        tf = DataFrame(row)
        ttf=repeat(tf, Dates.value.(row.EndDate-row.StartDate) + 1)
        ttf.DayDate = ttf.StartDate .+ Dates.Day.(0:nrow(ttf)-1)
        ttf.Start = ttf.DayDate .== ttf.StartDate
        ttf.End = ttf.DayDate .== ttf.EndDate
        append!(s,ttf, cols = :union)
    end
    n = nrow(s)
    println("$(n) day rows")
    @time sort!(s,[:IndividualId,:DayDate,order(:ResidentIndex, rev=true), :StartDate, order(:EndDate, rev=true)]);
    @time unique!(s,[:IndividualId,:DayDate]);
    n = nrow(s)
    println("$(n) unique rows")
    lastindividual = -1
    gap = 0
    n = nrow(s)
    for i = 1:n
        if lastindividual != s[i,:IndividualId]
            lastindividual=s[i,:IndividualId]
            gap = 0
        else
            lastgap = Dates.value(s[i,:DayDate]-s[i-1,:DayDate])
            nextgap = 0
            if i < n
                nextgap = s[i,:IndividualId] != s[i+1,:IndividualId] ? 0 : Dates.value(s[i+1,:DayDate]-s[i,:DayDate])
            end
            if lastgap>1
                s[i,:GapEnd] = 1
                gap = gap == 0 ? 1 : 0
            end
            if nextgap>1
                s[i,:GapStart]=1
            end
            s[i,:Gap] = gap
        end
    end
    df = combine(groupby(s, [:IndividualId,:Gap,:LocationId]), :DayDate => minimum => :StartDate, :StartType => first => :StartType, 
                                                               :DayDate => maximum => :EndDate, :EndType => last => :EndType, 
                                                               :GapStart => maximum => :GapStart, :GapEnd => maximum => :GapEnd,
                                                               :ResidentIndex => mean => :ResidentIndex)
end #test
df = test()