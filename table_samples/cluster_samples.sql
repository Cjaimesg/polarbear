create or replace table PruebaClusterNumerica as
select 
    rank() over (order by seq8()) as ID,
    RANDOM() as any_other_columns
from table(generator(rowcount => 3000000000))

union all
select 
    rank() over (order by seq8()) * -1 as ID,
    RANDOM() as any_other_columns
from table(generator(rowcount => 300000000))

union all
select 
    rank() over (order by seq8()) * 3 as ID,
    RANDOM() as any_other_columns
from table(generator(rowcount => 3000000000))

union all
select 
    rank() over (order by seq8()) * -3 as ID,
    RANDOM() as any_other_columns
from table(generator(rowcount => 300000000))

union all
select 
    rank() over (order by seq8()) * 10 as ID,
    RANDOM() as any_other_columns
from table(generator(rowcount => 3000000000))
;

create or replace table PruebaClusterFecha as
select 
    rank() over (order by seq8()) as ID,
    dateadd(day, uniform(0, 3650, random()), '2000-01-01') as fecha,
    RANDOM() as any_other_columns
from table(generator(rowcount => 3000000000))

union all
select 
    rank() over (order by seq8()) * -1 as ID,
    dateadd(day, uniform(0, 3650, random()), '2000-01-01') as fecha,
    RANDOM() as any_other_columns
from table(generator(rowcount => 300000000))

union all
select 
    rank() over (order by seq8()) * 3 as ID,
    dateadd(day, uniform(0, 3650, random()), '2000-01-01') as fecha,
    RANDOM() as any_other_columns
from table(generator(rowcount => 3000000000))

union all
select 
    rank() over (order by seq8()) * -3 as ID,
    dateadd(day, uniform(0, 3650, random()), '2000-01-01') as fecha,
    RANDOM() as any_other_columns
from table(generator(rowcount => 300000000))

union all
select 
    rank() over (order by seq8()) * 10 as ID,
    dateadd(day, uniform(0, 3650, random()), '2000-01-01') as fecha,
    RANDOM() as any_other_columns
from table(generator(rowcount => 3000000000))
;
create or replace table PruebaClusterTexto as
select 
    rank() over (order by seq8()) as ID,
    substr(md5(random()), 1, 10) as texto,
    RANDOM() as any_other_columns
from table(generator(rowcount => 3000000000))

union all
select 
    rank() over (order by seq8()) * -1 as ID,
    substr(md5(random()), 1, 10) as texto,
    RANDOM() as any_other_columns
from table(generator(rowcount => 300000000))

union all
select 
    rank() over (order by seq8()) * 3 as ID,
    substr(md5(random()), 1, 10) as texto,
    RANDOM() as any_other_columns
from table(generator(rowcount => 3000000000))

union all
select 
    rank() over (order by seq8()) * -3 as ID,
    substr(md5(random()), 1, 10) as texto,
    RANDOM() as any_other_columns
from table(generator(rowcount => 300000000))

union all
select 
    rank() over (order by seq8()) * 10 as ID,
    substr(md5(random()), 1, 10) as texto,
    RANDOM() as any_other_columns
from table(generator(rowcount => 3000000000))
;


-- Consulta 1: Búsqueda por ID positivo
SELECT * FROM PruebaClusterNumerica WHERE ID = 3323123;

-- Consulta 2: Búsqueda por ID negativo
SELECT * FROM PruebaClusterNumerica WHERE ID = -1234567;

-- Consulta 3: Búsqueda por ID múltiplo de 3
SELECT * FROM PruebaClusterNumerica WHERE ID = 9999999;

-- Consulta 4: Búsqueda por ID múltiplo de -3
SELECT * FROM PruebaClusterNumerica WHERE ID = -3333333;

-- Consulta 5: Búsqueda por ID múltiplo de 10
SELECT * FROM PruebaClusterNumerica WHERE ID = 10000000;

-- Consulta 6: Búsqueda por ID en el rango 1 a 1000000
SELECT * FROM PruebaClusterNumerica WHERE ID = 500000;

-- Consulta 7: Búsqueda por ID negativo en el rango -1 a -1000000
SELECT * FROM PruebaClusterNumerica WHERE ID = -500000;

-- Consulta 8: Búsqueda por ID mayor que 5000000
SELECT * FROM PruebaClusterNumerica WHERE ID = 7000000;

-- Consulta 9: Búsqueda por ID menor que -5000000
SELECT * FROM PruebaClusterNumerica WHERE ID = -7000000;

-- Consulta 10: Búsqueda por ID entre -100000 y 100000
SELECT * FROM PruebaClusterNumerica WHERE ID = 50000;

-- Consulta 11: Búsqueda por ID entre -100000 y 100000
SELECT * FROM PruebaClusterNumerica WHERE ID = -50000;

-- Consulta 12: Búsqueda por ID par positivo
SELECT * FROM PruebaClusterNumerica WHERE ID = 2468000;

-- Consulta 13: Búsqueda por ID par negativo
SELECT * FROM PruebaClusterNumerica WHERE ID = -2468000;

-- Consulta 14: Búsqueda por ID impar positivo
SELECT * FROM PruebaClusterNumerica WHERE ID = 1357901;

-- Consulta 15: Búsqueda por ID impar negativo
SELECT * FROM PruebaClusterNumerica WHERE ID = -1357901;

-- Consulta 16: Búsqueda por ID múltiplo de 5 positivo
SELECT * FROM PruebaClusterNumerica WHERE ID = 5000005;

-- Consulta 17: Búsqueda por ID múltiplo de 5 negativo
SELECT * FROM PruebaClusterNumerica WHERE ID = -5000005;

-- Consulta 18: Búsqueda por ID exacto
SELECT * FROM PruebaClusterNumerica WHERE ID = 123456789;

-- Consulta 19: Búsqueda por otro ID exacto
SELECT * FROM PruebaClusterNumerica WHERE ID = -987654321;

-- Consulta 20: Búsqueda por ID cercano a cero
SELECT * FROM PruebaClusterNumerica WHERE ID = 1;

-- Consulta 1: Búsqueda por fecha específica
SELECT * FROM PruebaClusterFecha WHERE fecha = '2005-06-15';

-- Consulta 2: Búsqueda por fecha en un rango específico
SELECT * FROM PruebaClusterFecha WHERE fecha BETWEEN '2003-01-01' AND '2003-12-31';

-- Consulta 3: Búsqueda por fecha exacta
SELECT * FROM PruebaClusterFecha WHERE fecha = '2008-09-25';

-- Consulta 4: Búsqueda por fecha en otro rango específico
SELECT * FROM PruebaClusterFecha WHERE fecha BETWEEN '2007-01-01' AND '2007-12-31';

-- Consulta 5: Búsqueda por fecha en un día específico
SELECT * FROM PruebaClusterFecha WHERE fecha = '2002-11-30';

-- Consulta 6: Búsqueda por fecha en otro rango de años
SELECT * FROM PruebaClusterFecha WHERE fecha BETWEEN '2000-01-01' AND '2000-12-31';

-- Consulta 7: Búsqueda por fecha en otro día específico
SELECT * FROM PruebaClusterFecha WHERE fecha = '2001-05-20';

-- Consulta 8: Búsqueda por fecha en otro rango de años
SELECT * FROM PruebaClusterFecha WHERE fecha BETWEEN '2004-01-01' AND '2004-12-31';

-- Consulta 9: Búsqueda por fecha en un rango de meses específicos
SELECT * FROM PruebaClusterFecha WHERE fecha BETWEEN '2002-06-01' AND '2002-06-30';

-- Consulta 10: Búsqueda por fecha en otro rango de meses específicos
SELECT * FROM PruebaClusterFecha WHERE fecha BETWEEN '2003-09-01' AND '2003-09-30';

-- Consulta 11: Búsqueda por fecha exacta
SELECT * FROM PruebaClusterFecha WHERE fecha = '2006-03-15';

-- Consulta 12: Búsqueda por fecha en otro rango de años
SELECT * FROM PruebaClusterFecha WHERE fecha BETWEEN '2008-01-01' AND '2008-12-31';

-- Consulta 13: Búsqueda por fecha en un día exacto
SELECT * FROM PruebaClusterFecha WHERE fecha = '2007-07-07';

-- Consulta 14: Búsqueda por fecha en otro día exacto
SELECT * FROM PruebaClusterFecha WHERE fecha = '2009-11-11';

-- Consulta 15: Búsqueda por fecha en un rango de años específicos
SELECT * FROM PruebaClusterFecha WHERE fecha BETWEEN '2001-01-01' AND '2001-12-31';

-- Consulta 16: Búsqueda por fecha en un rango de días específicos
SELECT * FROM PruebaClusterFecha WHERE fecha BETWEEN '2003-02-01' AND '2003-02-28';

-- Consulta 17: Búsqueda por fecha exacta
SELECT * FROM PruebaClusterFecha WHERE fecha = '2004-12-25';

-- Consulta 18: Búsqueda por fecha en otro rango de meses
SELECT * FROM PruebaClusterFecha WHERE fecha BETWEEN '2006-10-01' AND '2006-10-31';

-- Consulta 19: Búsqueda por fecha exacta
SELECT * FROM PruebaClusterFecha WHERE fecha = '2000-08-08';

-- Consulta 20: Búsqueda por fecha en un rango de meses exactos
SELECT * FROM PruebaClusterFecha WHERE fecha BETWEEN '2005-04-01' AND '2005-04-30';

-- Consulta 1: Búsqueda por texto específico
SELECT * FROM PruebaClusterTexto WHERE texto = 'a1b2c3d4e5';

-- Consulta 2: Búsqueda por otro texto específico
SELECT * FROM PruebaClusterTexto WHERE texto = 'f6g7h8i9j0';

-- Consulta 3: Búsqueda por texto con una subcadena específica
SELECT * FROM PruebaClusterTexto WHERE texto LIKE 'abc%';

-- Consulta 4: Búsqueda por texto con otra subcadena específica
SELECT * FROM PruebaClusterTexto WHERE texto LIKE 'xyz%';

-- Consulta 5: Búsqueda por un texto diferente
SELECT * FROM PruebaClusterTexto WHERE texto = 'z9y8x7w6v5';

-- Consulta 6: Búsqueda por otro texto específico
SELECT * FROM PruebaClusterTexto WHERE texto = 'k1l2m3n4o5';

-- Consulta 7: Búsqueda por otro texto específico
SELECT * FROM PruebaClusterTexto WHERE texto = 'p9q8r7s6t5';

-- Consulta 8: Búsqueda por texto con otra subcadena específica
SELECT * FROM PruebaClusterTexto WHERE texto LIKE 'mno%';

-- Consulta 9: Búsqueda por texto con otra subcadena específica
SELECT * FROM PruebaClusterTexto WHERE texto LIKE 'rst%';

-- Consulta 10: Búsqueda por otro texto específico
SELECT * FROM PruebaClusterTexto WHERE texto = 'u1v2w3x4y5';

-- Consulta 11: Búsqueda por texto específico
SELECT * FROM PruebaClusterTexto WHERE texto = 'd3e4f5g6h7';

-- Consulta 12: Búsqueda por texto específico
SELECT * FROM PruebaClusterTexto WHERE texto = 'i8j9k0l1m2';

-- Consulta 13: Búsqueda por texto con subcadena al inicio
SELECT * FROM PruebaClusterTexto WHERE texto LIKE 'lmn%';

-- Consulta 14: Búsqueda por texto con subcadena al inicio
SELECT * FROM PruebaClusterTexto WHERE texto LIKE 'opq%';

-- Consulta 15: Búsqueda por otro texto específico
SELECT * FROM PruebaClusterTexto WHERE texto = 'r2s3t4u5v6';

-- Consulta 16: Búsqueda por otro texto específico
SELECT * FROM PruebaClusterTexto WHERE texto = 'w7x8y9z0a1';

-- Consulta 17: Búsqueda por otro texto específico
SELECT * FROM PruebaClusterTexto WHERE texto = 'b3c4d5e6f7';

-- Consulta 18: Búsqueda por otro texto específico
SELECT * FROM PruebaClusterTexto WHERE texto = 'g8h9i0j1k2';

-- Consulta 19: Búsqueda por otro texto específico
SELECT * FROM PruebaClusterTexto WHERE texto = 'm4n5o6p7q8';

-- Consulta 20: Búsqueda por otro texto específico
SELECT * FROM PruebaClusterTexto WHERE texto = 'r9s0t1u2v3';