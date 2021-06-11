using Dates: iterate
using SAPRINCore
using Dates
using Arrow
using DataFrames
using ShiftedArrays
using XLSX
using FreqTables

function yragedel_episodes(node)
    residentdaybatches = Arrow.Stream(joinpath(dayextractionpath(node), "DayDatasetStep02_batched.arrow"))
    hstate = iterate(residentdaybatches)
    deliverydaybatches = Arrow.Stream(joinpath(dayextractionpath(node), "DeliveryDays_batched.arrow"))
    dstate = iterate(deliverydaybatches)
    i = 1
    while hstate !== nothing
        t = now()
        @info "Node $(node) batch $(i) at $(t)"
        h, hst = hstate
        hd = h |> DataFrame
        hd = transform!(hd, :DayDate => (x -> Dates.year.(x)) => :CalendarYear, [:DoB,:DayDate] => ((x,y) -> age.(x,y)) => :Age)
        #get delivery days
        d, dst = dstate
        dd = d |> DataFrame
        hd = leftjoin(hd, dd, on = [:IndividualId => :IndividualId, :DayDate => :DayDate])
        transform!(hd, :ChildrenBorn => ByRow(x -> ismissing(x) ? 0 : x) => :ChildrenBorn, :ChildrenEverBorn => ByRow(x -> ismissing(x) ? 0 : x) => :ChildrenEverBorn)
        e = combine(groupby(hd, [:IndividualId, :Episode, :CalendarYear, :Age, :ChildrenEverBorn]), :Resident => first => :Resident, :LocationId => first => :LocationId, :HouseholdId => first => :HouseholdId, 
                    :Sex => first => :Sex, :DoB => first => :DoB, :DoD => first => :DoD, :MotherId => first => :MotherId, :FatherId => first => :FatherId,
                    :DayDate => minimum => :StartDate, :DayDate => maximum => :EndDate, nrow => :Days, :ChildrenBorn => first => :ChildrenBorn,
                    :Born => first => :Born, :Enumeration => first => :Enumeration, :InMigration => first => :InMigration, :LocationEntry => first => :LocationEntry, :ExtResStart => first => :ExtResStart, :Participation => first => :Participation,
                    :Died => last => :Died, :OutMigration => last => :OutMigration, :LocationExit => last => :LocationExit, :ExtResEnd => last => :ExtResEnd, :Refusal => last => :Refusal, :LostToFollowUp => last => :LostToFollowUp, 
                    :Current => last => :Current, :MembershipStart => first => :MembershipStart, :MembershipEnd => last => :MembershipEnd, :Memberships => maximum => :Memberships, :GapStart => last => :Gap)
        filter!(row -> ismissing(row.DoD) || (!ismissing(row.DoD) && (row.StartDate <= row.DoD || row.DoB == row.DoD)), e) #Episode start must be less or equal than DoD, unless person born and died on the same day
        # Create episodes    
        episodes = transform(groupby(sort(e, [:IndividualId, :Episode, :CalendarYear, :Age, :ChildrenEverBorn]), [:IndividualId]), nrow => :Episodes, :IndividualId => eachindex => :episode, 
                   :CalendarYear => Base.Fix2(lead,1) => :NextYear, :CalendarYear => Base.Fix2(lag,1) => :PrevYear,
                   :Age => Base.Fix2(lead,1) => :NextAge, :Age => Base.Fix2(lag,1) => :PrevAge,
                   :ChildrenEverBorn => Base.Fix2(lag,1) => :PrevBorn)
        insertcols!(episodes, :Delete =>  Int16(0), :YrStart => Int16(0),:YrEnd => Int16(0), :AgeStart => Int16(0), :AgeEnd => Int16(0), :Delivery => Int16(0))
        select!(episodes, Not(:Episode))
        rename!(episodes, :episode => :Episode)
        lastIndividual = -1
        iscurrent = Int16(0)
        for row in eachrow(episodes)
            if !ismissing(row.DoD) && row.EndDate > row.DoD
                row.EndDate = row.DoD
                row.Days = Dates.value(row.EndDate - row.StartDate) + 1
                row.Died = Int16(1)
            end
            # Fix Flags
            if !ismissing(row.DoD) && row.DoD == row.EndDate
                row.Died = Int16(1)
            else
                row.Died = Int16(0)
            end
            if row.DoB == row.StartDate
                row.Born = Int16(1)
            else
                row.Born = Int16(0)
            end
            if row.Died == 1
                row.OutMigration = Int16(0)
                row.ExtResEnd = Int16(0)
                row.LocationExit = Int16(0)
                row.LostToFollowUp = Int16(0)
                row.Refusal = Int16(0)
                row.Current = Int16(0)
            end
            if row.Born == 1
                row.Enumeration = Int16(0)
                row.InMigration = Int16(0)
                row.LocationEntry = Int16(0)
                row.ExtResStart = Int16(0)
                row.MembershipStart = Int16(1)
            elseif row.Enumeration == 1
                row.InMigration = Int16(0)
                row.LocationEntry = Int16(0)
                row.ExtResStart = Int16(0)
                row.MembershipStart = Int16(1)
            elseif row.InMigration == 1
                row.LocationEntry = Int16(0)
                row.ExtResStart = Int16(0)
            end
            if row.LostToFollowUp == 1 || row.Refusal == 1 || row.Died == 1
                row.MembershipEnd = Int16(1)
            elseif row.Current == 1
                row.MembershipEnd = Int16(0)
            end
            if row.Episode == 1
                row.MembershipStart = Int16(1)
            end
            if row.ExtResEnd == 1 && row.LostToFollowUp == 1
                row.ExtResEnd = Int16(0)
            end
            if row.Episode != row.Episodes
                row.Current = Int16(0)
            end
            # Set Year Start flag
            if ismissing(row.PrevYear) && (Dates.month == 1 && Dates.day ==1)
                row.YrStart = Int16(1)
            elseif !ismissing(row.PrevYear) && (row.PrevYear != row.CalendarYear)    
                row.YrStart = Int16(1)
            end
            # Set Age Start flag
            if ismissing(row.PrevAge) && (row.DoB == row.StartDate)
                row.AgeStart = Int16(1)
            elseif !ismissing(row.PrevAge) && (row.PrevAge != row.Age)    
                row.AgeStart = Int16(1)
            end
            # Set Year End flag
            if ismissing(row.NextYear) && (Dates.month == 12 && Dates.day == 31)
                row.YrEnd = Int16(1)
            elseif !ismissing(row.NextYear) && (row.NextYear != row.CalendarYear)    
                row.YrEnd = Int16(1)
            end
            # Set Age End flag
            if ismissing(row.NextAge) && (row.EndDate == (row.DoB - Dates.Day(1)))
                row.AgeEnd = Int16(1)
            elseif !ismissing(row.NextAge) && (row.NextAge != row.Age)    
                row.AgeEnd = Int16(1)
            end
            # Set Delivery flag
            if ismissing(row.PrevBorn) && row.ChildrenBorn > 0
               row.Delivery = Int16(1) 
            elseif !ismissing(row.PrevBorn) && row.PrevBorn != row.ChildrenEverBorn
               row.Delivery = Int16(1) 
            end
            # mark for deletions all episodes after current
            if lastIndividual != row.IndividualId
                iscurrent = row.Current
                lastIndividual = row.IndividualId
            elseif iscurrent == 1
                row.Delete == 1
            else
                iscurrent = row.Current
            end
        end
        filter!(r -> r.Delete == 0, episodes)
        select!(episodes,Not([:Delete, :Episode, :Episodes, :PrevYear, :NextYear, :PrevAge, :NextAge, :PrevBorn]))
        e = transform(groupby(sort(episodes,[:IndividualId, :StartDate]), [:IndividualId]), :IndividualId => eachindex => :Episode, nrow => :Episodes)
        open(joinpath(episodepath(node), "SurveillanceEpisodesYrAgeDelivery$(i).arrow"),"w"; lock = true) do io
            Arrow.write(io, e, compress=:zstd)
        end
        @info "Node $(node) batch $(i) completed with $(nrow(e)) episodes after $(round(now()-t, Dates.Second))"
        hstate = iterate(residentdaybatches, hst)
        dstate = iterate(deliverydaybatches, dst)
        i = i + 1
    end
    combinebatches(episodepath(node),"SurveillanceEpisodesYrAgeDelivery", i-1)
    return nothing
end
"Do yr-age episodes QA"
function yragedel_episodeQA(node)
    df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesYrAgeDelivery_batched.arrow")) |> DataFrame
    # StartFlags
    sf = combine(groupby(df, [:Born, :Enumeration, :InMigration, :LocationEntry, :ExtResStart, :Participation, :MembershipStart, :YrStart, :AgeStart, :Delivery]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(sf, names(sf, Int16) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    XLSX.writetable(joinpath(episodepath(node),"QC", "EpisodesYrAgeDeliveryQA.xlsx"), collect(eachcol(sf)), names(sf), overwrite=true, sheetname="StartFlags")
    # EndFlags
    sf = combine(groupby(df, [:Died, :OutMigration, :LocationExit, :ExtResEnd, :Refusal, :LostToFollowUp, :Current, :MembershipEnd, :YrEnd, :AgeEnd]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(sf, names(sf, Int16) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node),"QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "EndFlags")
    # Episode1StartFlags
    sf = filter(r -> r.Episode == 1, df) #Start episodes only
    sf = combine(groupby(sf, [:Born, :Enumeration, :InMigration, :LocationEntry, :ExtResStart, :Participation, :MembershipStart, :YrStart, :AgeStart, :Delivery]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(sf, names(sf, Int16) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node),"QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "Episode1StartFlags")
    # LastEpisodeEndFlags
    sf = filter(r -> r.Episode == r.Episodes, df) #Last episodes only
    sf = combine(groupby(sf, [:Died, :OutMigration, :LocationExit, :ExtResEnd, :Refusal, :LostToFollowUp, :Current, :MembershipEnd, :YrEnd, :AgeEnd]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(sf, names(sf, Int16) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node),"QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "LastEpisodeEndFlags")
    # Births per year
    sf = filter(r -> r.Born == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "Births")
    # Enumerations per year
    sf = filter(r -> r.Enumeration == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "Enumerations")
    # InMigrations per year
    sf = filter(r -> r.InMigration == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "InMigrations")
    # LocationEntry per year
    sf = filter(r -> r.LocationEntry == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "LocationEntries")
    # ExtResStart per year
    sf = filter(r -> r.ExtResStart == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "ExtResStarts")
    # Deaths per year
    sf = filter(r -> r.Died == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "Deaths")
    # OutMigrations per year
    sf = filter(r -> r.OutMigration == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "OutMigrations")
    # LocationExits per year
    sf = filter(r -> r.LocationExit == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "LocationExits")
    # ExtResEnds per year
    sf = subset(df, :ExtResEnd => r -> r .== 1)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "ExtResEnds")
    # Refusals per year
    sf = subset(df, :Refusal => r -> r .== 1)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "Refusals")
    # LostToFollowUps per year
    sf = subset(df, :LostToFollowUp => r -> r .== 1)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "LostToFollowUps")
    # Deliveries per year
    sf = subset(df, :Delivery => r -> r .== 1)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols = false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "Deliveries")
    # ChildrenEverBorn
    sf = subset(df, [:Episode, :Episodes] => ByRow((x,y) -> x == y), :Sex => x -> x .== 2)
    sf = combine(groupby(sf, [:ChildrenEverBorn]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols = false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "ChildrenEverBorn")
    # Current per year
    sf = subset(df, :Current => r -> r .== 1)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "Current")
    #Current before end
    sf = filter(r -> r.Episode < r.Episodes & r.Current == 1, df)
    if nrow(sf) > 0
        transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
        transform!(sf, names(sf, Int16) .=> ByRow(Int), renamecols=false) #needed by XLSX
        addsheet!(joinpath(episodepath(node),"QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "CurrentBeforeEnd")
    end
    #Died before end
    sf = filter(r -> r.Episode < r.Episodes & r.Died == 1, df)
    if nrow(sf) > 0
        transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
        transform!(sf, names(sf, Int16) .=> ByRow(Int), renamecols=false) #needed by XLSX
        addsheet!(joinpath(episodepath(node),"QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "DiedBeforeEnd")
    end
    #Enumeration after episode 1
    sf = filter(r -> r.Episode > 1 & r.Enumeration == 1, df)
    transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(sf, names(sf, Int16) .=> ByRow(Int), renamecols=false) #needed by XLSX
    addsheet!(joinpath(episodepath(node),"QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "EnumerationAfterStart")
    #Died prior to or at end of episode, without Died flags
    sf = filter(r -> !ismissing(r.DoD) && r.DoD <= r.EndDate && r.Died == 0, df)
    if nrow(sf) > 0
        transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
        transform!(sf, names(sf, Int16) .=> ByRow(Int), renamecols=false) #needed by XLSX
        addsheet!(joinpath(episodepath(node),"QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "MissingDiedFlag")
    end
    #Died flag but no DoD
    sf = filter(r -> ismissing(r.DoD) && r.Died == 1, df)
    if nrow(sf) > 0
        transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
        transform!(sf, names(sf, Int16) .=> ByRow(Int), renamecols=false) #needed by XLSX
        addsheet!(joinpath(episodepath(node),"QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "DiedWithoutDoD")
    end
    #Died flag with ExtResEnd
    sf = filter(r -> r.Died == 1 && r.ExtResEnd == 1, df)
    if nrow(sf) > 0
        transform!(sf, names(sf, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
        transform!(sf, names(sf, Int16) .=> ByRow(Int), renamecols=false) #needed by XLSX
        addsheet!(joinpath(episodepath(node),"QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "DiedFlagWithExtResEnd")
    end
    #Episodes breakdown
    e = freqtable(df, :Episodes)
    addsheet!(joinpath(episodepath(node),"QC", "EpisodesYrAgeDeliveryQA.xlsx"), e, "EpisodesFreq")
    return nothing
end


@info "Started execution $(now())"
t = now()
df = yragedel_episodes("AHRI")
@info "Finished AHRI $(now())"
d = now()-t
@info "Stopped execution $(now()) duration $(round(d, Dates.Second))"
#yragedel_episodeQA
@info "Started execution $(now())"
t = now()
df = yragedel_episodeQA("AHRI")
@info "Finished AHRI QA $(now())"
d = now()-t
@info "Stopped execution $(now()) duration $(round(d, Dates.Second))"
