use polars::prelude::*;
use rerun::*;
use chrono::NaiveDate;
use rerun_earth::*;

#[derive(Default, Debug, Clone)]
pub struct Occurrence {
    decimal_latitude: f64,
    decimal_longitude: f64,
    year: i64,
    month: i64,
    day: i64,
    time: u64, // Epoch time
}

pub fn calc_epoch_time(year: i64, month: i64, day: i64) -> i64 {
    let date = NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32);
    match date {
        Some(date) => date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp(),
        None => -1,
    }
}

fn read_gbif_file(file: &str) -> Vec<Occurrence> {
    // Read the CSV file into a DataFrame

    let df = CsvReadOptions::default()
        .with_has_header(true)
        .with_parse_options(CsvParseOptions::default().with_separator(b'\t'))
        .with_infer_schema_length(Some(100_000))
        // .with_columns(vec!["decimalLatitude", "decimalLongitude", "year", "month", "day"].into())
        .try_into_reader_with_file_path(Some(file.into())).unwrap()        
        .finish().unwrap();

    // Print the DataFrame schema
    // println!("{:?}", df.schema());

    // name: decimalLatitude, field: Float64
    // name: decimalLongitude, field: Float64
    // name: year, field: Int64
    // name: month, field: Int64
    // name: day, field: Int64

    let df_len = df.height();
    let mut occurrences: Vec<Occurrence> = vec![Occurrence::default(); df_len];

    let mut invalid_entries = Vec::new();

    // Iterate over the rows of the DataFrame
    for col in df.iter() {
        // Polars is column based, so let's lean into that
        match col.name().to_string().as_str() {
            "decimalLatitude" => {
                for (i, value) in col.f64().unwrap().into_iter().enumerate() {
                    match value {
                        Some(v) => occurrences[i].decimal_latitude = v,
                        None => invalid_entries.push(i),
                    }
                }
            }
            "decimalLongitude" => {
                for (i, value) in col.f64().unwrap().into_iter().enumerate() {
                    match value {
                        Some(v) => occurrences[i].decimal_longitude = v,
                        None => invalid_entries.push(i),
                    }
                }
            }
            "year" => {
                for (i, value) in col.i64().unwrap().into_iter().enumerate() {
                    occurrences[i].year = value.unwrap_or(1970);
                }
            }
            "month" => {
                for (i, value) in col.i64().unwrap().into_iter().enumerate() {
                    occurrences[i].month = value.unwrap_or(1);
                }
            }
            "day" => {
                for (i, value) in col.i64().unwrap().into_iter().enumerate() {
                    occurrences[i].day = value.unwrap_or(1);
                }
            }
            _ => (),
        }
    }

    // Remove invalid entries
    // Unique invalid_entries
    invalid_entries.sort_unstable();
    invalid_entries.dedup();

    for i in invalid_entries.iter().rev() {
        occurrences.remove(*i);
    }

    // Convert ytd to epoch time
    for occurrence in occurrences.iter_mut() {
        occurrence.time = calc_epoch_time(occurrence.year, occurrence.month, occurrence.day) as u64;
    }

    occurrences
}

fn main() {
    // tiget_shark/occurrence.txt
    let tiger_shark_occurrences = read_gbif_file("tiger_shark/occurrence.txt");
    let great_white_shark_occurrences = read_gbif_file("great_white/records-2024-10-23.tsv");   

    let sphere_radius = 6_371_000.0; // Earth's mean radius in meters
    let max_subdivision_length = 100_000.0; // 100 km
    let subdivision_depth = 2; // Adjust depth as needed

    let rec = rerun::RecordingStreamBuilder::new("earth_example")
        .connect()
        .expect("Error connecting to Rerun");

        plot_shapefile(
            &rec,
            "land",
            "/mnt/data/development/rerun-earth/110m_land/ne_110m_land.shp",
            0x00FF00FF,
            sphere_radius,
            max_subdivision_length,
            subdivision_depth,
        );
    
        plot_shapefile(
            &rec,
            "ocean",
            "/mnt/data/development/rerun-earth/110m_ocean/ne_110m_ocean.shp",
            0x0000FFFF,
            sphere_radius,
            max_subdivision_length,
            subdivision_depth,
        );

    let temporal_timeline = Timeline::new_temporal("Tiger Shark Sightings");
    println!("Found {} occurrences", tiger_shark_occurrences.len());
    for (i, occurrence) in tiger_shark_occurrences.iter().enumerate() {
        // Convert lat and lon
        let loc = lat_lon_to_xyz(occurrence.decimal_latitude, occurrence.decimal_longitude, sphere_radius * 1.02);
        // Convert loc to f32 tuple
        let mut loc = (loc[0] as f32, loc[1] as f32, loc[2] as f32);
        let time = Time::from_seconds_since_epoch(occurrence.time as f64);

        let timepoint = TimePoint::default();
        let timepoint = timepoint.with(temporal_timeline, time);
        rec.set_timepoint(timepoint);

        // Create a Points3D
        let points = Points3D::new(vec![loc])
            .with_radii([100_000.0])
            .with_colors([0xFF0000FF]);

        rec.log(format!("tigershark/{i}"), &points).expect("Error logging points");
    }        

    println!("Found {} occurrences", tiger_shark_occurrences.len());
    for (i, occurrence) in great_white_shark_occurrences.iter().enumerate() {
        // Convert lat and lon
        let loc = lat_lon_to_xyz(occurrence.decimal_latitude, occurrence.decimal_longitude, sphere_radius * 1.02);
        // Convert loc to f32 tuple
        let mut loc = (loc[0] as f32, loc[1] as f32, loc[2] as f32);
        let time = Time::from_seconds_since_epoch(occurrence.time as f64);

        let timepoint = TimePoint::default();
        let timepoint = timepoint.with(temporal_timeline, time);
        rec.set_timepoint(timepoint);

        // Create a Points3D
        let points = Points3D::new(vec![loc])
            .with_radii([100_000.0])
            // Let's color these white
            .with_colors([0xFFFFFFFF]);

        rec.log(format!("greatwhiteshark/{i}"), &points).expect("Error logging points");
    }        

}


/*
Schema:
name: gbifID, field: Int64
name: accessRights, field: String
name: bibliographicCitation, field: String
name: language, field: String
name: license, field: String
name: modified, field: String
name: publisher, field: String
name: references, field: String
name: rightsHolder, field: String
name: type, field: String
name: institutionID, field: String
name: collectionID, field: String
name: datasetID, field: String
name: institutionCode, field: String
name: collectionCode, field: String
name: datasetName, field: String
name: ownerInstitutionCode, field: String
name: basisOfRecord, field: String
name: informationWithheld, field: String
name: dataGeneralizations, field: String
name: dynamicProperties, field: String
name: occurrenceID, field: String
name: catalogNumber, field: String
name: recordNumber, field: String
name: recordedBy, field: String
name: recordedByID, field: String
name: individualCount, field: Int64
name: organismQuantity, field: String
name: organismQuantityType, field: String
name: sex, field: String
name: lifeStage, field: String
name: reproductiveCondition, field: String
name: caste, field: String
name: behavior, field: String
name: vitality, field: String
name: establishmentMeans, field: String
name: degreeOfEstablishment, field: String
name: pathway, field: String
name: georeferenceVerificationStatus, field: String
name: occurrenceStatus, field: String
name: preparations, field: String
name: disposition, field: String
name: associatedOccurrences, field: String
name: associatedReferences, field: String
name: associatedSequences, field: String
name: associatedTaxa, field: String
name: otherCatalogNumbers, field: String
name: occurrenceRemarks, field: String
name: organismID, field: String
name: organismName, field: String
name: organismScope, field: String
name: associatedOrganisms, field: String
name: previousIdentifications, field: String
name: organismRemarks, field: String
name: materialEntityID, field: String
name: materialEntityRemarks, field: String
name: verbatimLabel, field: String
name: materialSampleID, field: String
name: eventID, field: String
name: parentEventID, field: String
name: eventType, field: String
name: fieldNumber, field: String
name: eventDate, field: String
name: eventTime, field: String
name: startDayOfYear, field: Int64
name: endDayOfYear, field: Int64
name: year, field: Int64
name: month, field: Int64
name: day, field: Int64
name: verbatimEventDate, field: String
name: habitat, field: String
name: samplingProtocol, field: String
name: sampleSizeValue, field: String
name: sampleSizeUnit, field: String
name: samplingEffort, field: String
name: fieldNotes, field: String
name: eventRemarks, field: String
name: locationID, field: String
name: higherGeographyID, field: String
name: higherGeography, field: String
name: continent, field: String
name: waterBody, field: String
name: islandGroup, field: String
name: island, field: String
name: countryCode, field: String
name: stateProvince, field: String
name: county, field: String
name: municipality, field: String
name: locality, field: String
name: verbatimLocality, field: String
name: verbatimElevation, field: String
name: verticalDatum, field: String
name: verbatimDepth, field: String
name: minimumDistanceAboveSurfaceInMeters, field: String
name: maximumDistanceAboveSurfaceInMeters, field: String
name: locationAccordingTo, field: String
name: locationRemarks, field: String
name: decimalLatitude, field: Float64
name: decimalLongitude, field: Float64
name: coordinateUncertaintyInMeters, field: Float64
name: coordinatePrecision, field: String
name: pointRadiusSpatialFit, field: String
name: verbatimCoordinateSystem, field: String
name: verbatimSRS, field: String
name: footprintWKT, field: String
name: footprintSRS, field: String
name: footprintSpatialFit, field: String
name: georeferencedBy, field: String
name: georeferencedDate, field: String
name: georeferenceProtocol, field: String
name: georeferenceSources, field: String
name: georeferenceRemarks, field: String
name: geologicalContextID, field: String
name: earliestEonOrLowestEonothem, field: String
name: latestEonOrHighestEonothem, field: String
name: earliestEraOrLowestErathem, field: String
name: latestEraOrHighestErathem, field: String
name: earliestPeriodOrLowestSystem, field: String
name: latestPeriodOrHighestSystem, field: String
name: earliestEpochOrLowestSeries, field: String
name: latestEpochOrHighestSeries, field: String
name: earliestAgeOrLowestStage, field: String
name: latestAgeOrHighestStage, field: String
name: lowestBiostratigraphicZone, field: String
name: highestBiostratigraphicZone, field: String
name: lithostratigraphicTerms, field: String
name: group, field: String
name: formation, field: String
name: member, field: String
name: bed, field: String
name: identificationID, field: String
name: verbatimIdentification, field: String
name: identificationQualifier, field: String
name: typeStatus, field: String
name: identifiedBy, field: String
name: identifiedByID, field: String
name: dateIdentified, field: String
name: identificationReferences, field: String
name: identificationVerificationStatus, field: String
name: identificationRemarks, field: String
name: taxonID, field: String
name: scientificNameID, field: String
name: acceptedNameUsageID, field: String
name: parentNameUsageID, field: String
name: originalNameUsageID, field: String
name: nameAccordingToID, field: String
name: namePublishedInID, field: String
name: taxonConceptID, field: String
name: scientificName, field: String
name: acceptedNameUsage, field: String
name: parentNameUsage, field: String
name: originalNameUsage, field: String
name: nameAccordingTo, field: String
name: namePublishedIn, field: String
name: namePublishedInYear, field: String
name: higherClassification, field: String
name: kingdom, field: String
name: phylum, field: String
name: class, field: String
name: order, field: String
name: superfamily, field: String
name: family, field: String
name: subfamily, field: String
name: tribe, field: String
name: subtribe, field: String
name: genus, field: String
name: genericName, field: String
name: subgenus, field: String
name: infragenericEpithet, field: String
name: specificEpithet, field: String
name: infraspecificEpithet, field: String
name: cultivarEpithet, field: String
name: taxonRank, field: String
name: verbatimTaxonRank, field: String
name: vernacularName, field: String
name: nomenclaturalCode, field: String
name: taxonomicStatus, field: String
name: nomenclaturalStatus, field: String
name: taxonRemarks, field: String
name: datasetKey, field: String
name: publishingCountry, field: String
name: lastInterpreted, field: String
name: elevation, field: String
name: elevationAccuracy, field: String
name: depth, field: Float64
name: depthAccuracy, field: Float64
name: distanceFromCentroidInMeters, field: String
name: issue, field: String
name: mediaType, field: String
name: hasCoordinate, field: Boolean
name: hasGeospatialIssues, field: Boolean
name: taxonKey, field: Int64
name: acceptedTaxonKey, field: Int64
name: kingdomKey, field: Int64
name: phylumKey, field: Int64
name: classKey, field: Int64
name: orderKey, field: Int64
name: familyKey, field: Int64
name: genusKey, field: Int64
name: subgenusKey, field: String
name: speciesKey, field: Int64
name: species, field: String
name: acceptedScientificName, field: String
name: verbatimScientificName, field: String
name: typifiedName, field: String
name: protocol, field: String
name: lastParsed, field: String
name: lastCrawled, field: String
name: repatriated, field: Boolean
name: relativeOrganismQuantity, field: String
name: projectId, field: String
name: isSequenced, field: Boolean
name: gbifRegion, field: String
name: publishedByGbifRegion, field: String
name: level0Gid, field: String
name: level0Name, field: String
name: level1Gid, field: String
name: level1Name, field: String
name: level2Gid, field: String
name: level2Name, field: String
name: level3Gid, field: String
name: level3Name, field: String
name: iucnRedListCategory, field: String

*/