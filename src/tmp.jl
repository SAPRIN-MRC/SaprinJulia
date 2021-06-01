using SAPRINCore
using Dates
using Arrow
using DataFrames
using ShiftedArrays

function processdeliverydays(motherId, deliveryDate, nextDelivery, endDate, childrenBorn, childrenEverBorn)
    stop = endDate[1]
    if !ismissing(nextDelivery[1])
        stop = nextDelivery[1] - Day(1)
    end
    if endDate[1] < stop
        stop = endDate[1]
    end
    start = deliveryDate[1]
    # if motherId[1] == 3452
    #     println("Delivery dates $(deliveryDate)")
    #     println("MotherDoD $(motherDoD)")
    #     println("start $(start) stop $(stop)")
    # end
    res_daydate = collect(start:Day(1):stop)
    res_childrenBorn = fill(0, length(res_daydate))
    res_childrenBorn[1] = childrenBorn[1]
    res_childrenEverBorn = fill(childrenEverBorn[1], length(res_daydate))
    for i in 2:length(deliveryDate)
        stop = endDate[i]
        if !ismissing(nextDelivery[i])
            stop = nextDelivery[i] - Day(1)
        end
        if endDate[i] < stop
            stop = endDate[i]
        end
        if deliveryDate[i] > res_daydate[end]
            start = deliveryDate[i]
        elseif nextDelivery[i] > res_daydate[end]
            start = res_daydate[end] + Day(1)
        else
            continue #this episode is contained within the previous episode
        end
        # if motherId[i] == 3452
        #     println("MotherDoD $(motherDoD)")
        #     println("start $(start) stop $(stop)")
        # end
        new_daydate = start:Day(1):stop
        append!(res_daydate, new_daydate)
        new_childrenborn = fill(0, length(new_daydate))
        new_childrenborn[1] = childrenBorn[i]
        append!(res_childrenBorn, new_childrenborn)
        append!(res_childrenEverBorn,fill(childrenEverBorn[i], length(new_daydate)))
    end
    return (daydate = res_daydate, childrenBorn = res_childrenBorn, childrenEverBorn = res_childrenEverBorn)
end

function getdeliverydays(basedirectory::String, node::String, f, batch::Int64)
    println("Batch $(batch) $(nrow(f)) episodes to extract")
    dfs = sort(f, [:DeliveryDate])
    dropmissing!(dfs,[:MotherId])
    gdf = groupby(dfs, :MotherId, sort=true)
    s = combine(gdf, [:MotherId, :DeliveryDate, :NextDelivery, :EndDate, :ChildrenBorn, :ChildrenEverBorn] => processdeliverydays => AsTable)
    rename!(s,Dict(:daydate => "DayDate", :childrenBorn => "ChildrenBorn", :childrenEverBorn => "ChildrenEverBorn"))
    open(joinpath(basedirectory,node,"DayExtraction","DeliveryDays$(batch).arrow"),"w"; lock = true) do io
        Arrow.write(io, s, compress=:zstd)
    end
    return nothing
end
#Create deliveries dataset
function deliverydays(node)
    individuals = open(joinpath(stagingpath(node), "Individuals.arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end
    mothers = dropmissing(individuals, [:MotherId])
    deliveries = combine(groupby(mothers, [:MotherId, :DoB]), nrow => :ChildrenBorn)
    rename!(deliveries, :DoB => :DeliveryDate)
    sort!(deliveries)
    transform!(groupby(deliveries,[:MotherId]),:ChildrenBorn => cumsum => :ChildrenEverBorn)
    # Get Start and End bounds from episodes
    df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesBasic_batched.arrow")) |> DataFrame
    bounds = combine(groupby(df, :IndividualId), :StartDate => minimum => :StartDate, :EndDate => maximum => :EndDate)
    deliveries = innerjoin(deliveries, bounds, on = :MotherId => :IndividualId)
    sort!(deliveries,[:MotherId, :DeliveryDate])
    transform!(groupby(deliveries, :MotherId), :DeliveryDate => Base.Fix2(lead, 1) => :NextDelivery)
    # Deliveries prior to first observation of mother
    earlydeliveries = subset(deliveries, [:DeliveryDate, :StartDate] => (x,y) -> x .< y)
    select!(earlydeliveries, :MotherId, :StartDate => :DeliveryDate, :ChildrenBorn, :ChildrenEverBorn, :StartDate, :EndDate, :NextDelivery)
    earlydeliveries.ChildrenBorn .= 0
    earlydeliveries = combine(groupby(earlydeliveries, :MotherId), :DeliveryDate => first => :DeliveryDate, :ChildrenBorn => first => :ChildrenBorn, 
                      :ChildrenEverBorn => maximum => :ChildrenEverBorn, :StartDate => first => :StartDate,
                      :EndDate => first => :EndDate, :NextDelivery => maximum => :NextDelivery)
    subset!(deliveries, [:DeliveryDate, :StartDate] => (x,y) -> x .>= y, [:DeliveryDate, :EndDate] => (x,y) -> x .<= y)
    deliveries = vcat(deliveries, earlydeliveries)
    sort!(deliveries,[:MotherId, :DeliveryDate])
    minId, maxId, batches = individualbatch(settings.BaseDirectory, node, BatchSize)
    for i = 1:batches
        fromId, toId = nextidrange(minId, maxId, i, BatchSize)
        @info "Batch $(i) from $(fromId) to $(toId)"
        d = filter([:MotherId] => id -> fromId <= id <= toId, deliveries)
        getdeliverydays(settings.BaseDirectory, node, d, i)
    end
    combinebatches(settings.BaseDirectory, node, "DayExtraction", "DeliveryDays",batches)
    return deliveries
end

@info "Started execution $(now())"
t = now()
df = deliverydays("DIMAMO")
@info "Finished DIMAMO $(now())"
d = now()-t
@info "Stopped execution $(now()) duration $(round(d, Dates.Second))"
