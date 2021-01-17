using Arrow
using Dates
using DataFrames

function process(startdate, enddate, locationid)
    start = startdate[1]
    stop = enddate[1]
    location = locationid[1]
   res_daydate = collect(start:Day(1):stop)
    res_startdate = fill(start, length(res_daydate))
    res_enddate = fill(stop, length(res_daydate))
    res_location = fill(location, length(res_daydate))
    episode = 1
    res_episode = fill(episode, length(res_daydate))
    for i in 2:length(startdate)
        if startdate[i] > res_daydate[end]
            start = startdate[i]
        elseif enddate[i] > res_daydate[end]
            start = res_daydate[end] + Day(1)
        else
            continue #this episode is contained within the previous episode
        end
        episode = episode + 1
        stop = enddate[i]
        location = locationid[i]
        new_daydate = start:Day(1):stop
        append!(res_daydate, new_daydate)
        append!(res_startdate, fill(startdate[i], length(new_daydate)))
        append!(res_enddate, fill(stop, length(new_daydate)))
        append!(res_location, fill(location, length(new_daydate)))
        append!(res_episode, fill(episode, length(new_daydate)))
    end

    return (daydate=res_daydate, startdate=res_startdate, enddate=res_enddate, locationid=res_location, episode = res_episode)
end

function eliminateoverlap()
    df = DataFrame(id = [1,1,2,3,3,4,4], startdate = [Date(2018,3,1),Date(2019,4,2),Date(2018,6,4),Date(2018,5,1), Date(2019,5,1), Date(2012,1,1), Date(2012,2,2)], 
                   enddate = [Date(2019,4,4),Date(2019,8,5),Date(2019,3,1),Date(2019,4,15),Date(2019,6,15),Date(2012,6,30), Date(2012,2,10)], locationid=[10,11,21,30,30,40,41])
    dfs = sort(df, [:startdate, order(:enddate, rev=true)])
    gdf = groupby(dfs, :id, sort=true)
    r = combine(gdf, [:startdate, :enddate, :locationid] => process => AsTable)
    df = combine(groupby(r, [:id,:episode,:locationid]), :daydate => minimum => :StartDate, :daydate => maximum => :EndDate, :episode => maximum => :episodes)
    return df
end

df = eliminateoverlap()