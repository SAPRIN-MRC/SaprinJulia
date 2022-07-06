
SELECT
  [HouseholdItem],
  [NbrOwned],
  COUNT(*) n
FROM [dbo].[HHOwnedItems] O
GROUP BY HouseholdItem, NbrOwned
ORDER BY HouseholdItem, NbrOwned;
--
SELECT
  [Livestock],
  COUNT(*) n
FROM [dbo].[HHStatusObservations]
GROUP BY Livestock;
--
SELECT
  LivestockSheep,
  COUNT(*) n
FROM [dbo].[HHStatusObservations]
GROUP BY LivestockSheep;
--
WITH LiveStock AS (
	SELECT
	  H.uuid HouseholdUid,
	  EM.EventUid ObservationUid,
	  CAST(28 AS int) AS AssetId,
	  CAST(1 AS int) AS AssetStatusId
	FROM dbo.Households H
	  JOIN dbo.HHStatusObservations HS ON H.IntID = HS.Household
	  JOIN dbo.EventsMapping EM ON HS.Visit = EM.EventId
	WHERE HS.Livestock IN ('B','P','S')
	UNION
	SELECT
	  H.uuid HouseholdUid,
	  EM.EventUid ObservationUid,
	  CAST(27 AS int) AS AssetId,
	  CAST(1 AS int) AS AssetStatusId
	FROM dbo.Households H
	  JOIN dbo.HHStatusObservations HS ON H.IntID = HS.Household
	  JOIN dbo.EventsMapping EM ON HS.Visit = EM.EventId
	WHERE HS.LivestockCattle BETWEEN 1 AND 200
	UNION
	SELECT
	  H.uuid HouseholdUid,
	  EM.EventUid ObservationUid,
	  CAST(28 AS int) AS AssetId,
	  CAST(1 AS int) AS AssetStatusId
	FROM dbo.Households H
	  JOIN dbo.HHStatusObservations HS ON H.IntID = HS.Household
	  JOIN dbo.EventsMapping EM ON HS.Visit = EM.EventId
	WHERE HS.LivestockSheep BETWEEN 1 AND 200
	UNION
	SELECT
	  H.uuid HouseholdUid,
	  EM.EventUid ObservationUid,
	  CAST(28 AS int) AS AssetId,
	  CAST(1 AS int) AS AssetStatusId
	FROM dbo.Households H
	  JOIN dbo.HHStatusObservations HS ON H.IntID = HS.Household
	  JOIN dbo.EventsMapping EM ON HS.Visit = EM.EventId
	WHERE HS.LivestockGoats BETWEEN 1 AND 200
	UNION
	SELECT
	  H.uuid HouseholdUid,
	  EM.EventUid ObservationUid,
	  CAST(28 AS int) AS AssetId,
	  CAST(1 AS int) AS AssetStatusId
	FROM dbo.Households H
	  JOIN dbo.HHStatusObservations HS ON H.IntID = HS.Household
	  JOIN dbo.EventsMapping EM ON HS.Visit = EM.EventId
	WHERE HS.LivestockOther BETWEEN 1 AND 200
	UNION
	SELECT
	  H.uuid HouseholdUid,
	  EM.EventUid ObservationUid,
	  CAST(28 AS int) AS AssetId,
	  CAST(1 AS int) AS AssetStatusId
	FROM dbo.Households H
	  JOIN dbo.HHStatusObservations HS ON H.IntID = HS.Household
	  JOIN dbo.EventsMapping EM ON HS.Visit = EM.EventId
	WHERE HS.LivestockPigs BETWEEN 1 AND 200
)
SELECT DISTINCT
  HO.HouseholdObservationUid,
  LS.AssetId,
  LS.AssetStatusId
  INTO SaprinDb_AHRI202206.dbo.HouseholdAssetsTmp
FROM LiveStock LS
  JOIN SaprinDb_AHRI202206.dbo.HouseholdObservations HO ON LS.HouseholdUid = HO.HouseholdUid AND LS.ObservationUid = HO.ObservationUid;


