"Group day records into basic exposure records"
function basicepisodes(node)
    @info "Started basic episode creation for node $(node) $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    minId, maxId, batches = individualbatch(node)
    #residentdaybatches = Arrow.Stream(joinpath(dayextractionpath(node), "DayDatasetStep02_batched.arrow"))
    for batch = 1:batches
        t = now()
        @info "Node $(node) batch $(batch) at $(t)"
        hd = deserializefromfile(joinpath(dayextractionpath(node), "DayDatasetStep02$(batch)"))
        e = combine(groupby(hd, [:IndividualId, :Episode]), :Resident => first => :Resident, :LocationId => first => :LocationId, :HouseholdId => first => :HouseholdId,
            :Sex => first => :Sex, :DoB => first => :DoB, :DoD => first => :DoD, :MotherId => first => :MotherId, :FatherId => first => :FatherId,
            :DayDate => minimum => :StartDate, :DayDate => maximum => :EndDate, nrow => :Days,
            :Born => maximum => :Born, :Enumeration => maximum => :Enumeration, :InMigration => maximum => :InMigration, :LocationEntry => maximum => :LocationEntry, :ExtResStart => maximum => :ExtResStart, :Participation => maximum => :Participation,
            :Died => maximum => :Died, :OutMigration => maximum => :OutMigration, :LocationExit => maximum => :LocationExit, :ExtResEnd => maximum => :ExtResEnd, :Refusal => maximum => :Refusal, :LostToFollowUp => maximum => :LostToFollowUp,
            :Current => maximum => :Current, :MembershipStart => maximum => :MembershipStart, :MembershipEnd => maximum => :MembershipEnd, :Memberships => maximum => :Memberships, :GapStart => maximum => :Gap)
        filter!(row -> ismissing(row.DoD) || (!ismissing(row.DoD) && (row.StartDate < row.DoD || row.DoB == row.DoD)), e) #Episode start must be less than DoD, unless person born and died on the same day
        episodes = transform(groupby(sort(e, [:IndividualId, :Episode]), [:IndividualId]), nrow => :Episodes)
        insertcols!(episodes, :Delete => Int16(0))
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
                row.MembershipStart = Int8(1)
            end
            if row.ExtResEnd == 1 && row.LostToFollowUp == 1
                row.ExtResEnd = Int16(0)
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
        select!(episodes, Not([:Delete, :Episode, :Episodes]))
        e = transform(groupby(sort(episodes, [:IndividualId, :StartDate]), [:IndividualId]), :IndividualId => eachindex => :Episode, nrow => :Episodes)
        serializetofile(joinpath(episodepath(node), "SurveillanceEpisodesBasic$(batch)"), e)
        @info "Node $(node) batch $(batch) completed with $(nrow(e)) episodes after $(round(now()-t, Dates.Second))"
    end
    episodes = restoredataframe(episodepath(node), "SurveillanceEpisodesBasic", batches)
    open(joinpath(episodepath(node), "SurveillanceEpisodesBasic.arrow"), "w"; lock=true) do io
        Arrow.write(io, episodes, compress=:zstd)
    end
    deletebatchfiles(episodepath(node), "SurveillanceEpisodesBasic", batches)
    @info "=== Finished basic episode creation for node $(node) $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    return nothing
end
"Do basic episodes QA"
function basicepisodeQA(node)
    @info "Started basic episode QA for node $(node) $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesBasic.arrow")) |> DataFrame
    sf = combine(groupby(df, [:Born, :Enumeration, :InMigration, :LocationEntry, :ExtResStart, :Participation, :MembershipStart]), nrow => :n)
    sort!(sf)
    writeXLSX(joinpath(episodepath(node), "QC", "EpisodesQA.xlsx"), sf, "StartFlags")
    sf = combine(groupby(df, [:Died, :OutMigration, :LocationExit, :ExtResEnd, :Refusal, :LostToFollowUp, :Current, :MembershipEnd]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesQA.xlsx"), sf, "EndFlags")
    sf = filter(r -> r.Episode == 1, df) #Start episodes only
    sf = combine(groupby(sf, [:Born, :Enumeration, :InMigration, :LocationEntry, :ExtResStart, :Participation, :MembershipStart]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesQA.xlsx"), sf, "Episode1StartFlags")
    sf = filter(r -> r.Episode == r.Episodes, df) #Last episodes only
    sf = combine(groupby(sf, [:Died, :OutMigration, :LocationExit, :ExtResEnd, :Refusal, :LostToFollowUp, :Current, :MembershipEnd]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesQA.xlsx"), sf, "LastEpisodeEndFlags")
    #Current before end
    sf = filter(r -> r.Episode < r.Episodes & r.Current == 1, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesQA.xlsx"), sf, "CurrentBeforeEnd")
    end
    #Died before end
    sf = filter(r -> r.Episode < r.Episodes & r.Died == 1, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesQA.xlsx"), sf, "DiedBeforeEnd")
    end
    #Enumeration after episode 1
    sf = filter(r -> r.Episode > 1 & r.Enumeration == 1, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesQA.xlsx"), sf, "EnumerationAfterStart")
    end
    #Died prior to or at end of episode, without Died flags
    sf = filter(r -> !ismissing(r.DoD) && r.DoD <= r.EndDate && r.Died == 0, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesQA.xlsx"), sf, "MissingDiedFlag")
    end
    #Died flag but no DoD
    sf = filter(r -> ismissing(r.DoD) && r.Died == 1, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesQA.xlsx"), sf, "DiedWithoutDoD")
    end
    #Died flag with ExtResEnd
    sf = filter(r -> r.Died == 1 && r.ExtResEnd == 1, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesQA.xlsx"), sf, "DiedFlagWithExtResEnd")
    end
    #Episodes breakdown
    e = frequency(df, :Episodes)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesQA.xlsx"), e, "EpisodesFreq")
    @info "=== Finished basic episode QA for node $(node) $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    return nothing
end
"Group day records into basic exposure records"
function yrage_episodes(node)
    @info "Started Yr Age episode creation node $(node) $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    minId, maxId, batches = individualbatch(node)
    for batch in 1:batches
        t = now()
        @info "Node $(node) batch $(batch) at $(t)"
        hd = deserializefromfile(joinpath(dayextractionpath(node), "DayDatasetStep02$(batch)"))
        hd = transform!(hd, :DayDate => (x -> Dates.year.(x)) => :CalendarYear, [:DoB, :DayDate] => ((x, y) -> age.(x, y)) => :Age)
        # Debug
        # t = filter(row -> row.IndividualId == 11100 || row.IndividualId == 17829, hd)
        # open(joinpath(episodepath(node), "DebugTmp$(i).arrow"),"w"; lock = true) do io
        #     Arrow.write(io, t, compress=:zstd)
        # end
        # End Debug
        e = combine(groupby(hd, [:IndividualId, :Episode, :CalendarYear, :Age]), :Resident => first => :Resident, :LocationId => first => :LocationId, :HouseholdId => first => :HouseholdId,
            :Sex => first => :Sex, :DoB => first => :DoB, :DoD => first => :DoD, :MotherId => first => :MotherId, :FatherId => first => :FatherId,
            :DayDate => minimum => :StartDate, :DayDate => maximum => :EndDate, nrow => :Days,
            :Born => first => :Born, :Enumeration => first => :Enumeration, :InMigration => first => :InMigration, :LocationEntry => first => :LocationEntry, :ExtResStart => first => :ExtResStart, :Participation => first => :Participation,
            :Died => last => :Died, :OutMigration => last => :OutMigration, :LocationExit => last => :LocationExit, :ExtResEnd => last => :ExtResEnd, :Refusal => last => :Refusal, :LostToFollowUp => last => :LostToFollowUp,
            :Current => last => :Current, :MembershipStart => first => :MembershipStart, :MembershipEnd => last => :MembershipEnd, :Memberships => maximum => :Memberships, :GapStart => last => :Gap)
        # Debug
        # t = filter(row -> row.IndividualId == 11100 || row.IndividualId == 17829, e)
        # open(joinpath(episodepath(node), "DebugEpisodes1Tmp$(i).arrow"),"w"; lock = true) do io
        #     Arrow.write(io, t, compress=:zstd)
        # end
        # End Debug
        filter!(row -> ismissing(row.DoD) || (!ismissing(row.DoD) && (row.StartDate <= row.DoD || row.DoB == row.DoD)), e) #Episode start must be less or equal than DoD, unless person born and died on the same day
        # Debug
        # t = filter(row -> row.IndividualId == 11100 || row.IndividualId == 17829, e)
        # open(joinpath(episodepath(node), "DebugEpisodes2Tmp$(i).arrow"),"w"; lock = true) do io
        #     Arrow.write(io, t, compress=:zstd)
        # end
        # End Debug
        episodes = transform(groupby(sort(e, [:IndividualId, :Episode, :CalendarYear, :Age]), [:IndividualId]), nrow => :Episodes, :IndividualId => eachindex => :episode,
            :CalendarYear => ShiftedArrays.lead => :NextYear, :CalendarYear => ShiftedArrays.lag => :PrevYear,
            :Age => ShiftedArrays.lead => :NextAge, :Age => ShiftedArrays.lag => :PrevAge)
        # Debug
        # t = filter(row -> row.IndividualId == 11100 || row.IndividualId == 17829, episodes)
        # open(joinpath(episodepath(node), "DebugEpisodes3Tmp$(i).arrow"),"w"; lock = true) do io
        #     Arrow.write(io, t, compress=:zstd)
        # end
        # End Debug
        insertcols!(episodes, :Delete => Int16(0), :YrStart => Int16(0), :YrEnd => Int16(0), :AgeStart => Int16(0), :AgeEnd => Int16(0))
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
                row.MembershipStart = Int8(1)
            end
            if row.ExtResEnd == 1 && row.LostToFollowUp == 1
                row.ExtResEnd = Int16(0)
            end
            if row.Episode != row.Episodes
                row.Current = Int8(0)
            end
            # Set Year Start flag
            if ismissing(row.PrevYear) && (Dates.month == 1 && Dates.day == 1)
                row.YrStart = Int8(1)
            elseif !ismissing(row.PrevYear) && (row.PrevYear != row.CalendarYear)
                row.YrStart = Int8(1)
            end
            # Set Age Start flag
            if ismissing(row.PrevAge) && (row.DoB == row.StartDate)
                row.AgeStart = Int8(1)
            elseif !ismissing(row.PrevAge) && (row.PrevAge != row.Age)
                row.AgeStart = Int8(1)
            end
            # Set Year End flag
            if ismissing(row.NextYear) && (Dates.month == 12 && Dates.day == 31)
                row.YrEnd = Int8(1)
            elseif !ismissing(row.NextYear) && (row.NextYear != row.CalendarYear)
                row.YrEnd = Int8(1)
            end
            # Set Age End flag
            if ismissing(row.NextAge) && (row.EndDate == (row.DoB - Dates.Day(1)))
                row.AgeEnd = Int8(1)
            elseif !ismissing(row.NextAge) && (row.NextAge != row.Age)
                row.AgeEnd = Int8(1)
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
        select!(episodes, Not([:Delete, :Episode, :Episodes, :PrevYear, :NextYear, :PrevAge, :NextAge]))
        e = transform(groupby(sort(episodes, [:IndividualId, :StartDate]), [:IndividualId]), :IndividualId => eachindex => :Episode, nrow => :Episodes)
        serializetofile(joinpath(episodepath(node), "SurveillanceEpisodesYrAge$(batch)"), e)
        @info "Node $(node) batch $(batch) completed with $(nrow(e)) episodes after $(round(now()-t, Dates.Second))"
    end
    episodes = restoredataframe(episodepath(node), "SurveillanceEpisodesYrAge", batches)
    open(joinpath(episodepath(node), "SurveillanceEpisodesYrAge.arrow"), "w"; lock=true) do io
        Arrow.write(io, episodes, compress=:zstd)
    end
    deletebatchfiles(episodepath(node),"SurveillanceEpisodesYrAge", batches)
    @info "=== Finished Yr Age episode creation $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    return nothing
end
"Do yr-age episodes QA"
function yrage_episodeQA(node)
    @info "Started Yr Age episode QA $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesYrAge.arrow")) |> DataFrame
    # StartFlags
    sf = combine(groupby(df, [:Born, :Enumeration, :InMigration, :LocationEntry, :ExtResStart, :Participation, :MembershipStart, :YrStart, :AgeStart]), nrow => :n)
    sort!(sf)
    writeXLSX(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "StartFlags")
    # EndFlags
    sf = combine(groupby(df, [:Died, :OutMigration, :LocationExit, :ExtResEnd, :Refusal, :LostToFollowUp, :Current, :MembershipEnd, :YrEnd, :AgeEnd]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "EndFlags")
    # Episode1StartFlags
    sf = filter(r -> r.Episode == 1, df) #Start episodes only
    sf = combine(groupby(sf, [:Born, :Enumeration, :InMigration, :LocationEntry, :ExtResStart, :Participation, :MembershipStart, :YrStart, :AgeStart]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "Episode1StartFlags")
    # LastEpisodeEndFlags
    sf = filter(r -> r.Episode == r.Episodes, df) #Last episodes only
    sf = combine(groupby(sf, [:Died, :OutMigration, :LocationExit, :ExtResEnd, :Refusal, :LostToFollowUp, :Current, :MembershipEnd, :YrEnd, :AgeEnd]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "LastEpisodeEndFlags")
    # Births per year
    sf = filter(r -> r.Born == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "Births")
    # Enumerations per year
    sf = filter(r -> r.Enumeration == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "Enumerations")
    # InMigrations per year
    sf = filter(r -> r.InMigration == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "InMigrations")
    # LocationEntry per year
    sf = filter(r -> r.LocationEntry == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "LocationEntries")
    # ExtResStart per year
    sf = filter(r -> r.ExtResStart == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "ExtResStarts")
    # Deaths per year
    sf = filter(r -> r.Died == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "Deaths")
    # OutMigrations per year
    sf = filter(r -> r.OutMigration == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "OutMigrations")
    # LocationExits per year
    sf = filter(r -> r.LocationExit == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "LocationExits")
    # ExtResEnds per year
    sf = subset(df, :ExtResEnd => r -> r .== 1)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "ExtResEnds")
    # Refusals per year
    sf = subset(df, :Refusal => r -> r .== 1)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "Refusals")
    # LostToFollowUps per year
    sf = subset(df, :LostToFollowUp => r -> r .== 1)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "LostToFollowUps")
    # Current per year
    sf = subset(df, :Current => r -> r .== 1)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "Current")
    #Current before end
    sf = filter(r -> r.Episode < r.Episodes & r.Current == 1, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "CurrentBeforeEnd")
    end
    #Died before end
    sf = filter(r -> r.Episode < r.Episodes & r.Died == 1, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "DiedBeforeEnd")
    end
    #Enumeration after episode 1
    sf = filter(r -> r.Episode > 1 & r.Enumeration == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "EnumerationAfterStart")
    #Died prior to or at end of episode, without Died flags
    sf = filter(r -> !ismissing(r.DoD) && r.DoD <= r.EndDate && r.Died == 0, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "MissingDiedFlag")
    end
    #Died flag but no DoD
    sf = filter(r -> ismissing(r.DoD) && r.Died == 1, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "DiedWithoutDoD")
    end
    #Died flag with ExtResEnd
    sf = filter(r -> r.Died == 1 && r.ExtResEnd == 1, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), sf, "DiedFlagWithExtResEnd")
    end
    #Episodes breakdown
    e = frequency(df, :Episodes)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeQA.xlsx"), e, "EpisodesFreq")
    @info "=== Finished Yr Age episode QA node $(node) $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    return nothing
end
"""
Create individual exposure episodes split on calendar year, age, and child birth (delivery)
"""
function yragedel_episodes(node)
    @info "Started Year Age Delivery episode creation $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    minId, maxId, batches = individualbatch(node)
    for i in 1:batches
        t = now()
        @info "Node $(node) batch $(i) at $(t)"
        hd = deserializefromfile(joinpath(dayextractionpath(node), "DayDatasetStep02$(i)"))
        transform!(hd, :DayDate => (x -> Dates.year.(x)) => :CalendarYear, [:DoB, :DayDate] => ((x, y) -> age.(x, y)) => :Age)
        #get delivery days
        dd = deserializefromfile(joinpath(dayextractionpath(node), "DeliveryDays$(i)"))
        hd = leftjoin(hd, dd, on=[:IndividualId => :IndividualId, :DayDate => :DayDate])
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
            :CalendarYear => ShiftedArrays.lead => :NextYear, :CalendarYear => ShiftedArrays.lag => :PrevYear,
            :Age => ShiftedArrays.lead => :NextAge, :Age => ShiftedArrays.lag => :PrevAge,
            :ChildrenEverBorn => ShiftedArrays.lag => :PrevBorn)
        insertcols!(episodes, :Delete => Int16(0), :YrStart => Int16(0), :YrEnd => Int16(0), :AgeStart => Int16(0), :AgeEnd => Int16(0), :Delivery => Int16(0))
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
            if ismissing(row.PrevYear) && (Dates.month == 1 && Dates.day == 1)
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
        select!(episodes, Not([:Delete, :Episode, :Episodes, :PrevYear, :NextYear, :PrevAge, :NextAge, :PrevBorn]))
        e = transform(groupby(sort(episodes, [:IndividualId, :StartDate]), [:IndividualId]), :IndividualId => eachindex => :Episode, nrow => :Episodes)
        serializetofile(joinpath(episodepath(node), "SurveillanceEpisodesYrAgeDelivery$(i)"), e)
        @info "Node $(node) batch $(i) completed with $(nrow(e)) episodes after $(round(now()-t, Dates.Second))"
    end
    episodes = restoredataframe(episodepath(node), "SurveillanceEpisodesYrAgeDelivery", batches)
    open(joinpath(episodepath(node), "SurveillanceEpisodesYrAgeDelivery.arrow"), "w"; lock=true) do io
        Arrow.write(io, episodes, compress=:zstd)
    end
    deletebatchfiles(episodepath(node), "SurveillanceEpisodesYrAgeDelivery", batches)
    @info "=== Finished Year Age Delivery episode creation $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    return nothing
end
"Do yr-age-delivery episodes QA"
function yragedel_episodeQA(node)
    @info "Started Year Age Delivery episode QA $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesYrAgeDelivery.arrow")) |> DataFrame
    # StartFlags
    sf = combine(groupby(df, [:Born, :Enumeration, :InMigration, :LocationEntry, :ExtResStart, :Participation, :MembershipStart, :YrStart, :AgeStart, :Delivery]), nrow => :n)
    sort!(sf)
    writeXLSX(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "StartFlags")
    # EndFlags
    sf = combine(groupby(df, [:Died, :OutMigration, :LocationExit, :ExtResEnd, :Refusal, :LostToFollowUp, :Current, :MembershipEnd, :YrEnd, :AgeEnd]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "EndFlags")
    # Episode1StartFlags
    sf = filter(r -> r.Episode == 1, df) #Start episodes only
    sf = combine(groupby(sf, [:Born, :Enumeration, :InMigration, :LocationEntry, :ExtResStart, :Participation, :MembershipStart, :YrStart, :AgeStart, :Delivery]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "Episode1StartFlags")
    # LastEpisodeEndFlags
    sf = filter(r -> r.Episode == r.Episodes, df) #Last episodes only
    sf = combine(groupby(sf, [:Died, :OutMigration, :LocationExit, :ExtResEnd, :Refusal, :LostToFollowUp, :Current, :MembershipEnd, :YrEnd, :AgeEnd]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "LastEpisodeEndFlags")
    # Births per year
    sf = filter(r -> r.Born == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "Births")
    # Enumerations per year
    sf = filter(r -> r.Enumeration == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "Enumerations")
    # InMigrations per year
    sf = filter(r -> r.InMigration == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "InMigrations")
    # LocationEntry per year
    sf = filter(r -> r.LocationEntry == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "LocationEntries")
    # ExtResStart per year
    sf = filter(r -> r.ExtResStart == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "ExtResStarts")
    # Deaths per year
    sf = filter(r -> r.Died == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "Deaths")
    # OutMigrations per year
    sf = filter(r -> r.OutMigration == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "OutMigrations")
    # LocationExits per year
    sf = filter(r -> r.LocationExit == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "LocationExits")
    # ExtResEnds per year
    sf = subset(df, :ExtResEnd => r -> r .== 1)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "ExtResEnds")
    # Refusals per year
    sf = subset(df, :Refusal => r -> r .== 1)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "Refusals")
    # LostToFollowUps per year
    sf = subset(df, :LostToFollowUp => r -> r .== 1)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "LostToFollowUps")
    # Deliveries per year
    sf = subset(df, :Delivery => r -> r .== 1)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "Deliveries")
    # ChildrenEverBorn
    sf = subset(df, [:Episode, :Episodes] => ByRow((x, y) -> x == y), :Sex => x -> x .== 2)
    sf = combine(groupby(sf, [:ChildrenEverBorn]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "ChildrenEverBorn")
    # Current per year
    sf = subset(df, :Current => r -> r .== 1)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    sort!(sf)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "Current")
    #Current before end
    sf = filter(r -> r.Episode < r.Episodes & r.Current == 1, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "CurrentBeforeEnd")
    end
    #Died before end
    sf = filter(r -> r.Episode < r.Episodes & r.Died == 1, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "DiedBeforeEnd")
    end
    #Enumeration after episode 1
    sf = filter(r -> r.Episode > 1 & r.Enumeration == 1, df)
    sf = combine(groupby(sf, [:CalendarYear]), nrow => :n)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "EnumerationAfterStart")
    #Died prior to or at end of episode, without Died flags
    sf = filter(r -> !ismissing(r.DoD) && r.DoD <= r.EndDate && r.Died == 0, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "MissingDiedFlag")
    end
    #Died flag but no DoD
    sf = filter(r -> ismissing(r.DoD) && r.Died == 1, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "DiedWithoutDoD")
    end
    #Died flag with ExtResEnd
    sf = filter(r -> r.Died == 1 && r.ExtResEnd == 1, df)
    if nrow(sf) > 0
        addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), sf, "DiedFlagWithExtResEnd")
    end
    #Episodes breakdown
    e = frequency(df, :Episodes)
    addsheet!(joinpath(episodepath(node), "QC", "EpisodesYrAgeDeliveryQA.xlsx"), e, "EpisodesFreq")
    @info "=== Finished Year Age Delivery episode QA $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    return nothing
end

"Group day records into parent co-residency records"
function parentresidentepisodes(node)
    #residentdaybatches = Arrow.Stream(joinpath(dayextractionpath(node), "DayDatasetStep04_batched.arrow"))
    minId, maxId, batches = individualbatch(node)
    for i = 1:batches
        t = now()
        @info "Node $(node) batch $(i) at $(t)"
        hd = deserializefromfile(joinpath(dayextractionpath(node), "DayDatasetStep04$(i)"))
        e = combine(groupby(hd, [:IndividualId, :Episode, :MotherCoResident, :FatherCoResident]), :Resident => first => :Resident, :LocationId => first => :LocationId, :HouseholdId => first => :HouseholdId,
            :Sex => first => :Sex, :DoB => first => :DoB, :DoD => first => :DoD, :MotherId => first => :MotherId, :FatherId => first => :FatherId,
            :DayDate => minimum => :StartDate, :DayDate => maximum => :EndDate, nrow => :Days,
            :Born => maximum => :Born, :Enumeration => maximum => :Enumeration, :InMigration => maximum => :InMigration, :LocationEntry => maximum => :LocationEntry, :ExtResStart => maximum => :ExtResStart, :Participation => maximum => :Participation,
            :Died => maximum => :Died, :OutMigration => maximum => :OutMigration, :LocationExit => maximum => :LocationExit, :ExtResEnd => maximum => :ExtResEnd, :Refusal => maximum => :Refusal, :LostToFollowUp => maximum => :LostToFollowUp,
            :Current => maximum => :Current, :MembershipStart => maximum => :MembershipStart, :MembershipEnd => maximum => :MembershipEnd, :Memberships => maximum => :Memberships, :GapStart => maximum => :Gap)
        filter!(row -> ismissing(row.DoD) || (!ismissing(row.DoD) && (row.StartDate < row.DoD || row.DoB == row.DoD)), e) #Episode start must be less than DoD, unless person born and died on the same day
        episodes = transform(groupby(sort(e, [:IndividualId, :Episode]), [:IndividualId]), nrow => :Episodes)
        insertcols!(episodes, :Delete => Int8(0))
        lastIndividual = -1
        iscurrent = Int8(0)
        for row in eachrow(episodes)
            if !ismissing(row.DoD) && row.EndDate > row.DoD
                row.EndDate = row.DoD
                row.Days = Dates.value(row.EndDate - row.StartDate) + 1
                row.Died = Int8(1)
            end
            # Fix Flags
            if !ismissing(row.DoD) && row.DoD == row.EndDate
                row.Died = Int8(1)
            else
                row.Died = Int8(0)
            end
            if row.DoB == row.StartDate
                row.Born = Int8(1)
            else
                row.Born = Int8(0)
            end
            if row.Died == 1
                row.OutMigration = Int8(0)
                row.ExtResEnd = Int8(0)
                row.LocationExit = Int8(0)
                row.LostToFollowUp = Int8(0)
                row.Refusal = Int8(0)
                row.Current = Int8(0)
            end
            if row.Born == 1
                row.Enumeration = Int8(0)
                row.InMigration = Int8(0)
                row.LocationEntry = Int8(0)
                row.ExtResStart = Int8(0)
                row.MembershipStart = Int8(1)
            elseif row.Enumeration == 1
                row.InMigration = Int8(0)
                row.LocationEntry = Int8(0)
                row.ExtResStart = Int8(0)
                row.MembershipStart = Int8(1)
            elseif row.InMigration == 1
                row.LocationEntry = Int8(0)
                row.ExtResStart = Int8(0)
            end
            if row.LostToFollowUp == 1 || row.Refusal == 1 || row.Died == 1
                row.MembershipEnd = Int8(1)
            elseif row.Current == 1
                row.MembershipEnd = Int8(0)
            end
            if row.Episode == 1
                row.MembershipStart = Int8(1)
            end
            if row.ExtResEnd == 1 && row.LostToFollowUp == 1
                row.ExtResEnd = Int8(0)
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
        select!(episodes, Not([:Delete, :Episode, :Episodes]))
        e = transform(groupby(sort(episodes, [:IndividualId, :StartDate]), [:IndividualId]), :IndividualId => eachindex => :Episode, nrow => :Episodes)
        serializetofile(joinpath(episodepath(node), "SurveillanceEpisodesParentCoresident$(i)"),e)
        @info "Node $(node) batch $(i) completed with $(nrow(e)) episodes after $(round(now()-t, Dates.Second))"
    end
    episodes = restoredataframe(episodepath(node), "SurveillanceEpisodesParentCoresident", batches)
    open(joinpath(episodepath(node), "SurveillanceEpisodesParentCoresident.arrow"), "w"; lock=true) do io
        Arrow.write(io, episodes, compress=:zstd)
    end
    deletebatchfiles(episodepath(node), "SurveillanceEpisodesParentCoresident", batches)
    return nothing
end
"Determine parental status, 0 = unknown, 1 = coresident, 2 = alive living elsewhere, 3 = dead"
function parentstatus(parentdead, parentcoresident)
    if parentdead == 1
        return Int8(3)
    end
    if !ismissing(parentcoresident) && parentcoresident == 1
        return Int8(1)
    end
    if parentdead == 0
        return Int8(2)
    end
    return Int8(0)
end
"""
Create individual exposure episodes split on calendar year, age, child birth (delivery), and parental status
"""
function yragedelparentalstatus_episodes(node)
    @info "Started Year Age Delivery Parental Status episode creation $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    #residentdaybatches = Arrow.Stream(joinpath(dayextractionpath(node), "DayDatasetStep04_batched.arrow"))
    #deliverydaybatches = Arrow.Stream(joinpath(dayextractionpath(node), "DeliveryDays_batched.arrow"))
    minId, maxId, batches = individualbatch(node)
    for i = 1:batches
        t = now()
        @info "Node $(node) batch $(i) at $(t)"
        hd = deserializefromfile(joinpath(dayextractionpath(node), "DayDatasetStep04$(i)"))
        hd = transform!(hd, :DayDate => (x -> Dates.year.(x)) => :CalendarYear, [:DoB, :DayDate] => ((x, y) -> age.(x, y)) => :Age,
            [:MotherDead, :MotherCoResident] => ((x, y) -> parentstatus.(x, y)) => :MotherStatus,
            [:FatherDead, :FatherCoResident] => ((x, y) -> parentstatus.(x, y)) => :FatherStatus)
        #get delivery days
        dd = deserializefromfile(joinpath(dayextractionpath(node), "DeliveryDays$(i)"))
        hd = leftjoin(hd, dd, on=[:IndividualId => :IndividualId, :DayDate => :DayDate])
        transform!(hd, :ChildrenBorn => ByRow(x -> ismissing(x) ? 0 : x) => :ChildrenBorn, :ChildrenEverBorn => ByRow(x -> ismissing(x) ? 0 : x) => :ChildrenEverBorn)
        e = combine(groupby(hd, [:IndividualId, :Episode, :CalendarYear, :Age, :ChildrenEverBorn, :MotherStatus, :FatherStatus]), :Resident => first => :Resident, :LocationId => first => :LocationId, :HouseholdId => first => :HouseholdId,
            :Sex => first => :Sex, :DoB => first => :DoB, :DoD => first => :DoD, :MotherId => first => :MotherId, :FatherId => first => :FatherId,
            :DayDate => minimum => :StartDate, :DayDate => maximum => :EndDate, nrow => :Days, :ChildrenBorn => first => :ChildrenBorn,
            :Born => first => :Born, :Enumeration => first => :Enumeration, :InMigration => first => :InMigration, :LocationEntry => first => :LocationEntry, :ExtResStart => first => :ExtResStart, :Participation => first => :Participation,
            :Died => last => :Died, :OutMigration => last => :OutMigration, :LocationExit => last => :LocationExit, :ExtResEnd => last => :ExtResEnd, :Refusal => last => :Refusal, :LostToFollowUp => last => :LostToFollowUp,
            :Current => last => :Current, :MembershipStart => first => :MembershipStart, :MembershipEnd => last => :MembershipEnd, :Memberships => maximum => :Memberships,
            :HHRelationshipTypeId => first => :HHRelationshipTypeId, :GapStart => last => :Gap)
        filter!(row -> ismissing(row.DoD) || (!ismissing(row.DoD) && (row.StartDate <= row.DoD || row.DoB == row.DoD)), e) #Episode start must be less or equal than DoD, unless person born and died on the same day
        # Create episodes    
        episodes = transform(groupby(sort(e, [:IndividualId, :Episode, :CalendarYear, :Age, :ChildrenEverBorn, :StartDate]), [:IndividualId]), nrow => :Episodes, :IndividualId => eachindex => :episode,
            :CalendarYear => ShiftedArrays.lead => :NextYear, :CalendarYear => ShiftedArrays.lag => :PrevYear,
            :Age => ShiftedArrays.lead => :NextAge, :Age => ShiftedArrays.lag => :PrevAge,
            :ChildrenEverBorn => ShiftedArrays.lag => :PrevBorn,
            :MotherStatus => ShiftedArrays.lag => :PrevMotherStatus, :FatherStatus => ShiftedArrays.lag => :PrevFatherStatus)
        insertcols!(episodes, :Delete => Int16(0), :YrStart => Int16(0), :YrEnd => Int16(0), :AgeStart => Int16(0), :AgeEnd => Int16(0), :Delivery => Int16(0), :ParentStatusChanged => Int16(0))
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
            if ismissing(row.PrevYear) && (Dates.month == 1 && Dates.day == 1)
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
            # Set ParentStatusChanged flag
            if (!ismissing(row.PrevMotherStatus) && !ismissing(row.PrevFatherStatus)) &&
               (row.PrevMotherStatus != row.MotherStatus || row.PrevFatherStatus != row.FatherStatus)
                row.ParentStatusChanged = Int16(1)
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
        select!(episodes, Not([:Delete, :Episode, :Episodes, :PrevYear, :NextYear, :PrevAge, :NextAge, :PrevBorn, :PrevMotherStatus, :PrevFatherStatus]))
        e = transform(groupby(sort(episodes, [:IndividualId, :StartDate]), [:IndividualId]), :IndividualId => eachindex => :Episode, nrow => :Episodes)
        serializetofile(joinpath(episodepath(node), "SurveillanceEpisodesYrAgeDeliveryParents$(i)"), e)
        @info "Node $(node) batch $(i) completed with $(nrow(e)) episodes after $(round(now()-t, Dates.Second))"
    end
    episodes = restoredataframe(episodepath(node), "SurveillanceEpisodesYrAgeDeliveryParents", batches)
    open(joinpath(episodepath(node), "SurveillanceEpisodesYrAgeDeliveryParents.arrow"), "w"; lock=true) do io
        Arrow.write(io, episodes, compress=:zstd)
    end
    deletebatchfiles(episodepath(node), "SurveillanceEpisodesYrAgeDeliveryParents", batches)
    @info "=== Finished Year Age Delivery Parent status episode creation $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    return nothing
end
"Map episode start flag to StartType"
function mapstart(enumeration, born, participation, inmigration, locationentry, externalresstart)
    inmigration = inmigration != 1 ? locationentry : inmigration #locationentry treated as inmigration
    # find which flag is set
    z = findfirst(collect((born, enumeration, inmigration, externalresstart, participation)) .== 1)
    return z == nothing ? 6 : z #if no flag is set return 6 = attributechange
end
"Map episode end flags to EndType"
function mapend(died, refusal, ltf, current, outmigration, locationexit, extresend)
    outmigration = outmigration != 1 ? locationexit : outmigration #locationentry treated as inmigration
    z = findfirst(collect((died, outmigration, extresend, refusal, ltf, 0, current)) .== 1)
    return z == nothing ? 6 : z #if no flag is set return 6 = attributechange
end
"""
Convert SAPRIN SurveillanceEpisodesYrAgeDeliveryParents to mental Health Data Prize format
"""
function produce_mhepisodes(node::String)
    nodeid = Int8(findfirst(values(settings.Nodes) .== node))
    df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesYrAgeDeliveryParents_batched.arrow")) |> DataFrame
    df.NodeId .= nodeid
    df.IsUrbanOrRural .= nodeid == 1 || nodeid == 3 ? Int8(1) : Int8(3)
    df.SpouseId .= missing
    e = select(df, :NodeId, :IndividualId, :DoB, :DoD, :CalendarYear, :Age, :Sex, :LocationId, :HouseholdId, :HHRelationshipTypeId => :HHRelation, :IsUrbanOrRural,
        :MotherId => ByRow(x -> x == 0 ? missing : x) => :MotherId, :FatherId, :SpouseId, :StartDate, :EndDate,
        [:Enumeration, :Born, :Participation, :InMigration, :LocationEntry, :ExtResStart] => ByRow((e, b, p, i, l, x) -> mapstart(e, b, p, i, l, x)) => :StartType,
        [:Died, :Refusal, :LostToFollowUp, :Current, :OutMigration, :LocationExit, :ExtResEnd] => ByRow((d, r, l, c, o, a, x) -> mapend(d, r, l, c, o, a, x)) => :EndType,
        :Episode, :Episodes, :Resident, :MotherStatus, :FatherStatus, :ChildrenEverBorn)
    open(joinpath(episodepath(node), "IndividualExposureEpisodes.arrow"), "w") do io
        Arrow.write(io, e, compress=:zstd)
    end
    e = nothing
    return nothing
end
