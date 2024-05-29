
# Fonction pour lire les données et renommer les colonnes si nécessaire

read_and_prepare_data <- function(produit_path, conversion_path) {
  produit_dta <- read_dta(produit_path)
  conversion_data <- read_excel(conversion_path)
  names(produit_dta)[names(produit_dta) == paste0(produit,"__id")] <- "produitID"
  names(produit_dta)[names(produit_dta) == paste0("s07Bq03b_", produit)] <- "uniteID"
  names(produit_dta)[names(produit_dta) == paste0("s07Bq03c_", produit)] <- "tailleID"
  list(produit = produit_dta, conversion_data = conversion_data)

}


# Fonction pour faire le merge des données
merge_data <- function(produit_dta, conversion_data, produit) {
  conversion_selected <- conversion_data[c("produitID", "uniteID", "tailleID", "poids")]
  names(produit_dta)[names(produit_dta) == paste0(produit,"__id")] <- "produitID"
  produit_merge <- merge(produit_dta, conversion_selected, by = c("produitID", "uniteID", "tailleID"), all.x = TRUE)
  produit_merge <- produit_merge %>% filter(!is.na(produit_merge[[paste0("s07Bq07a_", produit)]]))

  produit_merge$poids <- as.numeric(produit_merge$poids)
  produit_merge[[paste0("s07Bq03a_", produit)]] <- as.numeric(produit_merge[[paste0("s07Bq03a_", produit)]])
  produit_merge$qte_cons_kg <- (produit_merge$poids * produit_merge[[paste0("s07Bq03a_", produit)]]) / 1000

  return(produit_merge)
}
produit_merge <- merge_data(dta[["produit"]], dta[["conversion_data"]], produit)


# Fonction pour traiter les données d'achats
process_achats <- function(produit_merge, produit) {
  achats_cols <- c("produitID", paste0("s07Bq07a_", produit), paste0("s07Bq07b_", produit), paste0("s07Bq07c_", produit), paste0("s07Bq08_", produit),"poids")
  produit_achats <- produit_merge[achats_cols]
  colnames(produit_achats)[2:5] <- c("qte", "uniteID", "tailleID", "Valeur")
  produit_achats <- produit_achats %>%
    group_by(produitID, qte, uniteID, tailleID) %>%
    mutate(Valeur = mean(Valeur)) %>%
    ungroup() %>%
    unique()
  names(produit_merge)[names(produit_merge) == paste0("s07Bq03a_", produit)] <- "qte"
  produit_merge <- merge(produit_merge, produit_achats, by = c("produitID", "uniteID", "tailleID", "qte"), all.x = TRUE)
  taux_unmatching <- sum(is.na(produit_merge$Valeur)) / length(produit_merge$Valeur) * 100
  print(taux_unmatching)

  return(viandes_achats = produit_achats)
}


#Gestion des unmatchings
process_viandes_achats <- function(viandes_achats, conversion_selected, viande_mergeds) {
  library(dplyr)
  viandes_achats$pus_en_g <- as.numeric(viandes_achats$Valeur) / (as.numeric(viandes_achats$poids) * as.numeric(viandes_achats$qte))
  viande_achats_fin <- viandes_achats %>%
    select(produitID, pus_en_g) %>%
    group_by(produitID) %>%
    mutate(pus_en_g = mean(pus_en_g, na.rm = TRUE)) %>%
    distinct()

  viande_mergeds <- merge(viande_mergeds, viande_achats_fin, by = "produitID", all.x = TRUE)

  viande_mergeds$Valeur[is.na(viande_mergeds$Valeur)] <- viande_mergeds$pus_en_g[is.na(viande_mergeds$Valeur)] * viande_mergeds$poids[is.na(viande_mergeds$Valeur)]

  return(viande_mergeds)
}
prod_mergeds <- process_viandes_achats(datag,dta[["conversion_data"]],produit_merge )


# Fonction pour importer et merger la base EHCVM MOD
merge_ehcvm <- function(produit_merge, ehcvm_path) {
  ehcvmmod <- read_dta(ehcvm_path)
  base_fin_produit <- merge(produit_merge, ehcvmmod, by = "interview__key", all.x = TRUE)
  return(base_fin_produit)
}


# Fonction pour merger avec le tableau ANSD
merge_ansd <- function(base_fin_produit, ansd_path) {
  tableau_ansd <- read_excel(ansd_path)
  names(tableau_ansd)[names(tableau_ansd) == "Region_id"] <- "s00q01"
  base_fin_produit <- merge(base_fin_produit, tableau_ansd[c("Taille moyenne des ménages ordinaires", "s00q01")], by = "s00q01", all.x = TRUE)
  names(base_fin_produit)[names(base_fin_produit) == "Taille moyenne des ménages ordinaires"] <- "Taille_moyenne_menages"

  base_fin_produit$cons_moy_pers <- base_fin_produit$qte_cons_kg / base_fin_produit$Taille_moyenne_menages
  list(base_fin_produit= base_fin_produit, tableau_ansd = tableau_ansd)
}


# Fonction pour calculer les moyennes par région et milieu
calculate_means <- function(base_fin_produit, tableau_ansd) {
  base_fin_produit_reg <- base_fin_produit %>%
    group_by(s00q01) %>%
    summarise(cons_moy_pers = mean(cons_moy_pers, na.rm = TRUE))

  base_cons_moy_reg <- merge(base_fin_produit_reg, tableau_ansd[c("Région", "s00q01")], by = "s00q01", all.x = TRUE)

  base_fin_produit_mil <- base_fin_produit %>%
    group_by(s00q04) %>%
    summarise(cons_moy_pers = mean(cons_moy_pers, na.rm = TRUE))

  list(region_means = base_cons_moy_reg, milieu_means = base_fin_produit_mil)
}
prod_mergeds <- calculate_means(base_fin[["base_fin_produit"]],base_fin[["tableau_ansd"]])


