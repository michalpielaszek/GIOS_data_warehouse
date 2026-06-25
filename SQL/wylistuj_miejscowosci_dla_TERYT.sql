SELECT
    w.kod_wojewodztwa,
    p.kod_powiatu,
    g.kod_gminy,
    g.rodzaj_gminy,
    m.kod_simc,
    m.nazwa_miejscowosci
FROM gios.miejscowosc m
JOIN gios.gmina g
    ON m.gmina_id = g.gmina_id
JOIN gios.powiat p
    ON g.powiat_id = p.powiat_id
JOIN gios.wojewodztwo w
    ON p.wojewodztwo_id = w.wojewodztwo_id
WHERE w.kod_wojewodztwa = '04'
  AND p.kod_powiatu = '14'
  AND g.kod_gminy = '09' 
ORDER BY m.nazwa_miejscowosci;

AND g.rodzaj_gminy = '3'