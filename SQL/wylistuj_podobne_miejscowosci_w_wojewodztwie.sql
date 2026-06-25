SELECT
    w.kod_wojewodztwa,
    w.nazwa_wojewodztwa,
    p.kod_powiatu,
    p.nazwa_powiatu,
    g.kod_gminy,
    g.rodzaj_gminy,
    g.nazwa_gminy,
    m.kod_simc,
    m.nazwa_miejscowosci
FROM gios.miejscowosc m
JOIN gios.gmina g
    ON m.gmina_id = g.gmina_id
JOIN gios.powiat p
    ON g.powiat_id = p.powiat_id
JOIN gios.wojewodztwo w
    ON p.wojewodztwo_id = w.wojewodztwo_id
WHERE w.kod_wojewodztwa = '02'
  AND m.nazwa_miejscowosci ILIKE '%Kalin%'
ORDER BY p.nazwa_powiatu, g.nazwa_gminy, m.nazwa_miejscowosci;