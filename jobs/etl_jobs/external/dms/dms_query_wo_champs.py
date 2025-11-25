DMS_QUERY = """
query getDemarches($demarcheNumber: Int!, $after: String,  $updatedSince: ISO8601DateTime) {
    demarche(number:$demarcheNumber) {
        title
        dossiers(first:5, after: $after, updatedSince: $updatedSince) {
            edges {
                node {
                    ...DossierFragment
                }
                cursor
            }
            pageInfo {
                endCursor
                hasNextPage
            }
        }
    }
}

fragment DossierFragment on Dossier {
    id
    number
    archived
    state
    dateDerniereModification
    datePassageEnConstruction
    datePassageEnInstruction
    dateTraitement

    instructeurs {
        email
    }
    groupeInstructeur {
        id
        number
        label
    }
    demandeur {
        ... on PersonnePhysique {
            civilite
            nom
            prenom
            dateDeNaissance
        }
        ...PersonneMoraleFragment
    }
}


fragment PersonneMoraleFragment on PersonneMorale {
    siret
    siegeSocial
    naf
    libelleNaf
    address {
        ...AddressFragment
    }
    entreprise {
        siren
        capitalSocial
        numeroTvaIntracommunautaire
        formeJuridique
        formeJuridiqueCode
        nomCommercial
        raisonSociale
        siretSiegeSocial
        codeEffectifEntreprise
        dateCreation
        nom
        prenom
    }
    association {
        rna
        titre
        objet
        dateCreation
        dateDeclaration
        datePublication
    }
}

fragment AddressFragment on Address {
    label
    type
    streetAddress
    streetNumber
    streetName
    postalCode
    cityName
    cityCode
    departmentName
    departmentCode
    regionName
    regionCode
}
"""
