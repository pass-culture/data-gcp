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

    champs {
        ...ChampFragment
        ...RootChampFragment
    }
    annotations {
        ...ChampFragment
        ...RootChampFragment
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

fragment RootChampFragment on Champ {
    ... on RepetitionChamp {
        champs {
            ...ChampFragment
        }
    }
    ... on SiretChamp {
        etablissement {
            ...PersonneMoraleFragment
        }
    }
    ... on DossierLinkChamp {
        dossier {
            id
            state
            usager {
                email
            }
        }
    }
}

fragment ChampFragment on Champ {
    id
    label
    stringValue
    ... on LinkedDropDownListChamp {
        primaryValue
        secondaryValue
    }
    ... on MultipleDropDownListChamp {
        values
    }
    ... on AddressChamp {
        address {
            ...AddressFragment
        }
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
