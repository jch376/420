import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import numpy as np
from pretty import SparkPretty

pretty = SparkPretty(limit=5)

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

metadata = spark.read.csv("/data/msd/main/summary/metadata.csv.gz", header=True)

metdata.show()
# +----------------+-----------------+-------------------+-------------------+------------------+---------------+--------------------+----------------+--------------------+--------------------+---------------+-----+----------------+-------------------+--------------------+------------------+-------------------+------------------+--------------------+----------------+
# |analyzer_version|artist_7digitalid| artist_familiarity|  artist_hotttnesss|         artist_id|artist_latitude|     artist_location|artist_longitude|         artist_mbid|         artist_name|artist_playmeid|genre|idx_artist_terms|idx_similar_artists|             release|release_7digitalid|    song_hotttnesss|           song_id|               title|track_7digitalid|
# +----------------+-----------------+-------------------+-------------------+------------------+---------------+--------------------+----------------+--------------------+--------------------+---------------+-----+----------------+-------------------+--------------------+------------------+-------------------+------------------+--------------------+----------------+
# |            null|             4069| 0.6498221002008776| 0.3940318927141434|ARYZTJS1187B98C555|           null|                null|            null|357ff05d-848a-44c...|    Faster Pussy cat|          44895| null|               0|                  0|Monster Ballads X...|            633681| 0.5428987432910862|SOQMMHC12AB0180CB8|        Silent Night|         7032331|
# |            null|           113480| 0.4396039666767154| 0.3569921077564064|ARMVN3U1187FB3A1EB|           null|                null|            null|8d7ef530-a6fd-4f8...|    Karkkiautomaatti|             -1| null|               0|                  0|         Karkuteillä|            145266| 0.2998774882739778|SOVFVAK12A8C1350D9|         Tanssi vaan|         1514808|
# |            null|            63531| 0.6436805720579895| 0.4375038365946544|ARGEKB01187FB50750|        55.8578|   Glasgow, Scotland|        -4.24251|3d403d44-36ce-465...|      Hudson Mohawke|             -1| null|               0|                  0|              Butter|            625706| 0.6178709693948196|SOGTUKN12AB017F4F1|   No One Could Ever|         6945353|
# |            null|            65051|0.44850115965646636|0.37234906851712235|ARNWYLR1187B9B2F9C|           null|                null|            null|12be7648-7094-495...|         Yerba Brava|          34000| null|               0|                  0|             De Culo|            199368|               null|SOBNYVR12A8C13558C|       Si Vos Querés|         2168257|
# |            null|           158279|                0.0|                0.0|AREQDTE1269FB37231|           null|                null|            null|                null|          Der Mystic|             -1| null|               0|                  0|Rene Ablaze Prese...|            209038|               null|SOHSBXH12A8C13B0DF|    Tangle Of Aspens|         2264873|
# |            null|           219281|  0.361286979627774|0.10962584705877759|AR2NS5Y1187FB5879D|           null|                null|            null|d087b377-bab7-46c...|    David Montgomery|             -1| null|               0|                  0|Berwald: Symphoni...|            299244|               null|SOZVAPQ12A8C13B63C|"Symphony No. 1 G...|         3360982|
# |            null|             3736| 0.6929227305760355|  0.453731585998959|ARO41T51187FB397AB|           null| Mexico City, Mexico|            null|d2461c0a-5575-442...|  Sasha / Turbulence|           1396| null|               0|                  0|Strictly The Best...|             52968|               null|SOQVRHI12A6D4FB2D7|    We Have Got Love|          552626|
# |            null|            49941| 0.5881561876748532|0.40109242517712895|AR3Z9WY1187FB4CDC2|           null|                null|            null|bf61e8ff-7621-465...|          Kris Kross|           9594| null|               0|                  0|             Da Bomb|            580432|               null|SOEYRFT12AB018936C|   2 Da Beat Ch'yall|         6435649|
# |            null|            15202| 0.4084654634687691|0.28590119604546194|ARA04401187B991E6E|       54.99241|Londonderry, Nort...|        -7.31923|1a9bf859-1dc2-495...|        Joseph Locke|          61524| null|               0|                  0|           Danny Boy|            756677|               null|SOPMIYT12A6D4F851E|             Goodbye|         8376489|
# |            null|            76721|0.41994127477041937| 0.2491372295395512|ARCVMYS12454A51E6E|           null|                null|            null|                null|The Sun Harbor's ...|          50151| null|               0|                  0|March to cadence ...|             95069|               null|SOJCFMH12A8C13B0C2|Mama_ mama can't ...|         1043208|
# |            null|           483188| 0.5550139661044319|0.35294891386831184|AR59BSJ1187FB4474F|           null|                null|            null|891fccfc-24c1-4bf...|    3 Gars Su'l Sofa|             -1| null|               0|                  0|Des cobras des ta...|            649271|               null|SOYGNWH12AB018191E|       L'antarctique|         7192392|
# |            null|           182764| 0.5413897644832653|0.36909624998745993|ARCVIFR1187B99129F|       21.01841|Guanajuato, Guana...|      -101.25912|ec57c22f-9bb7-48a...|       Jorge Negrete|          16768| null|               0|                  0|32 Grandes Éxitos...|            714860|               null|SOLJTLX12AB01890ED|  El hijo del pueblo|         7928975|
# |            null|           165382| 0.6220053881178192|0.45157896286125104|ARVIT0V1187B9A7CDE|           null|                null|            null|2eb02f8c-8dfa-49d...|        Danny Diablo|             -1| null|               0|                  0|International Har...|            620618|0.39200877147130697|SOQQESG12A58A7AA28|Cold Beer feat. P...|         6893903|
# |            null|             5823| 0.6218264858073926| 0.4069847271546184|AREMPER1187B9AEB42|           null|                null|            null|14efbb08-c3d8-404...|           Tiger Lou|             -1| null|               0|                  0|           The Loyal|            204414| 0.4634896622372766|SOMPVQB12A8C1379BB|              Pilots|         2218694|
# |            null|           107401| 0.5436899174527013|0.37367918580172677|ARBAMQB1187FB3C650|           null|                null|            null|0bb5e108-b41d-46c...|     Waldemar Bastos|          21683| null|               0|                  0|Afropea 3 - Telli...|            218915| 0.4499402040427523|SOGPCJI12A8C13CCA0|              N Gana|         2387740|
# |            null|            63158| 0.5298191345674961|0.41022851115895437|ARSB5591187B99A848|       57.42635|    Vetlanda, Sweden|        15.08518|fba3e876-68f1-4a1...|     Lena Philipsson|             -1| null|               0|                  0|          Lena 20 År|            543019|0.21204540548371908|SOSDCFG12AB0184647|                 006|         6010886|
# |            null|             2752| 0.6855028320873736|0.44673296759009207|ARDW5AW1187FB55708|       42.78668|      Vermillion, SD|       -96.92803|42222090-c5e5-424...|        Shawn Colvin|           1431| null|               0|                  0|          Cover Girl|            282071| 0.2707759989463275|SOBARPM12A8C133DFF|(Looking For) The...|         3156269|
# |            null|             8066| 0.7344714135425402| 0.5119758329425405|ARGWPP11187B9AEF43|        38.8235|            Maryland|       -75.92381|f76167bb-c117-402...|         Dying Fetus|          51871| null|               0|                  0|Descend Into Depr...|            610151| 0.6147658092809959|SOKOVRQ12A8C142811|   Ethos of Coercion|         6782293|
# |            null|            21191| 0.7389959064546399| 0.5633668961490896|ARDT9VH1187B999C0B|           null|      South Carolina|            null|6b22de04-fb48-44a...|               Emery|          10889| null|               0|                  0|I'm Only A Man (B...|            143873| 0.7173190344273362|SOIMMJJ12AF72AD643|         Rock-N-Rule|         1501464|
# |            null|            68575|0.46714484494272307|0.40912707944880217|ARWFDED1187B9B9D71|           null|                null|            null|8ff7e1b4-18f8-48e...|        Los Ronaldos|             -1| null|               0|                  0|       La bola extra|            212920|               null|SOVMBTP12A8C13A8F6|       La bola extra|         2318206|
# +----------------+-----------------+-------------------+-------------------+------------------+---------------+--------------------+----------------+--------------------+--------------------+---------------+-----+----------------+-------------------+--------------------+------------------+-------------------+------------------+--------------------+----------------+
# only showing top 20 rows


metadata = metadata.repartition(8)

metadata.cache()

metadata.groupby("song_id").count().count()
# 998963

metadata.groupby("song_id").count().show()
# +------------------+-----+
# |           song_id|count|
# +------------------+-----+
# |SOUNYIJ12AB018258C|    1|
# |SOVNMQG12AB018156F|    1|
# |SOWMDQH12AB018D363|    1|
# |SOVLBQQ12AB017EAC8|    1|
# |SOOGTRQ12A67ADD43A|    1|
# |SOCYRWQ12AB018C144|    1|
# |SOGJKQF12A6D4FCE60|    1|
# |SOASSJX12AB0184A73|    1|
# |SOGCSNF12AF729BA7C|    1|
# |SOMIXUF12A58A7A0A8|    1|
# |SOKGVMU12A81C211CF|    1|
# |SOUYZGQ12AC468834B|    1|
# |SOLFPBY12AC4687942|    1|
# |SOWQGTY12A8C13BA52|    1|
# |SOIYCJS12A6D4F9897|    1|
# |SOMPEZJ12AB0183C48|    1|
# |SONYRCI12AB018B448|    1|
# |SOFXZNN12AB0186130|    1|
# |SOUAMAY12A8C140C60|    1|
# |SOPIZJY12A6D4F5F72|    1|
# +------------------+-----+
# only showing top 20 rows


metadata.count()
# 1000000

analysis = spark.read.csv("/data/msd/main/summary/analysis.csv.gz", header=True)

analysis = analysis.repartition(8)

analysis.show()
# +--------------------+--------------------+------------+---------+--------------+------+-------------------+--------------+--------------------+---------------+-----------------------+------------------+-----------------------+-------------------------+------------------------------+---------------------------+--------------------+------------------+-------------------+---------------------+----------------+---+--------------+--------+----+---------------+-----------------+-------+--------------+-------------------------+------------------+
# |analysis_sample_rate|           audio_md5|danceability| duration|end_of_fade_in|energy|idx_bars_confidence|idx_bars_start|idx_beats_confidence|idx_beats_start|idx_sections_confidence|idx_sections_start|idx_segments_confidence|idx_segments_loudness_max|idx_segments_loudness_max_time|idx_segments_loudness_start|idx_segments_pitches|idx_segments_start|idx_segments_timbre|idx_tatums_confidence|idx_tatums_start|key|key_confidence|loudness|mode|mode_confidence|start_of_fade_out|  tempo|time_signature|time_signature_confidence|          track_id|
# +--------------------+--------------------+------------+---------+--------------+------+-------------------+--------------+--------------------+---------------+-----------------------+------------------+-----------------------+-------------------------+------------------------------+---------------------------+--------------------+------------------+-------------------+---------------------+----------------+---+--------------+--------+----+---------------+-----------------+-------+--------------+-------------------------+------------------+
# |               22050|6c6b51155dc3e3ad5...|         0.0|185.65179|           0.0|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0| 11|           0.0|   -6.37|   0|            0.0|          185.652|  90.48|             3|                      1.0|TREXWDP12903CBBF33|
# |               22050|930bfa5af423a14eb...|         0.0|391.07873|           0.0|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  0|          0.58|  -6.596|   1|          0.402|           385.08|150.018|             4|                     0.11|TRJNPPP128F9335ACC|
# |               22050|f52c38d73199e1e07...|         0.0|491.51955|           0.0|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  7|         0.164|  -8.098|   1|          0.403|          474.616| 142.99|             4|                    0.275|TRLUVEF12903CE61A3|
# |               22050|7c6a672ad10f92ad2...|         0.0|183.74485|         0.213|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  0|         0.788|  -4.255|   1|          0.718|          173.813|165.005|             4|                    0.468|TRCWYND12903CE9911|
# |               22050|ba88160751b4ad81d...|         0.0|284.02893|           0.0|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  4|           0.5|  -5.964|   0|          0.319|          279.121|212.383|             7|                      0.0|TRABOTO12903CDF879|
# |               22050|5ac1a9c4fc27ca863...|         0.0|199.94077|           0.2|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  1|         0.235|   -9.44|   0|            0.3|           188.36|110.624|             4|                    0.828|TROZEBH128F92D1002|
# |               22050|d4d3474d674d00baa...|         0.0|193.14893|         0.392|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  3|         0.651| -14.624|   1|          0.715|          185.835| 93.436|             3|                      1.0|TREWKQB128F425F3B5|
# |               22050|a37ec0c8b7d255b7f...|         0.0|137.19465|         0.159|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  4|         0.045| -13.356|   0|          0.366|          133.039|198.668|             4|                     0.88|TRVXIFF12903CBCB18|
# |               22050|4f0b006448042b819...|         0.0|142.96771|           0.0|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  9|         0.292|  -6.375|   1|          0.313|          136.876| 88.523|             4|                    0.139|TROFSKX128F426E9DC|
# |               22050|83d8af4b4cdb228e2...|         0.0|235.96363|           0.0|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  7|         0.601| -14.886|   1|          0.703|          220.032| 74.045|             3|                    0.838|TRRKQWE128F424327C|
# |               22050|d1e1af4577d61bae4...|         0.0|318.30159|           0.0|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  2|         0.007|  -8.806|   1|          0.229|          318.302|124.088|             4|                      1.0|TRHQNNW12903CE329D|
# |               22050|93b7ee29ee83a3ba7...|         0.0|256.31302|         0.351|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  0|         0.404|  -3.531|   1|          0.556|          251.687| 88.496|             4|                      0.0|TRUVCMO12903CA4CF7|
# |               22050|63ed11a3c500a43c9...|         0.0|433.71057|         0.711|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  9|         0.165|  -9.669|   1|            0.0|           426.51|174.267|             5|                    0.188|TRLSBBY128F9315035|
# |               22050|89b8ba1a4304fb767...|         0.0|153.39057|           0.0|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  2|          0.19| -16.222|   1|           0.41|          148.428|125.189|             4|                    0.834|TRNGZET128F42942B5|
# |               22050|9ce1917fff5d86367...|         0.0|109.26975|           0.0|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  4|         0.446|  -4.861|   0|          0.432|          102.284|146.736|             4|                    0.032|TREDXKW128F930C508|
# |               22050|ecb6a8eb10963fc60...|         0.0|252.31628|         0.229|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  8|         0.629|  -14.45|   0|          0.635|          239.525|124.756|             4|                    0.798|TRGAVWN128F4247149|
# |               22050|bd9611bccf2a7f52c...|         0.0|325.25016|         0.069|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  7|         0.638|  -8.941|   0|          0.283|          299.265| 112.99|             4|                    0.787|TRHXTVS128F92DDCBA|
# |               22050|9eefe56f0273ff586...|         0.0|209.94567|         0.131|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  5|         0.304| -17.955|   0|          0.604|          197.265| 98.988|             3|                      1.0|TRXAGDS128F42410C8|
# |               22050|e1e3e89963b4f1987...|         0.0| 174.8371|           0.0|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  6|          0.53| -11.555|   1|          0.749|          167.683|151.827|             4|                    0.344|TRYRGWS128E078999B|
# |               22050|de9de6aed2b87cf8f...|         0.0|236.45995|           0.0|   0.0|                  0|             0|                   0|              0|                      0|                 0|                      0|                        0|                             0|                          0|                   0|                 0|                  0|                    0|               0|  1|         0.645| -13.534|   1|          0.578|          234.458| 94.825|             4|                    0.477|TRFTQNY128F933DCCE|
# +--------------------+--------------------+------------+---------+--------------+------+-------------------+--------------+--------------------+---------------+-----------------------+------------------+-----------------------+-------------------------+------------------------------+---------------------------+--------------------+------------------+-------------------+---------------------+----------------+---+--------------+--------+----+---------------+-----------------+-------+--------------+-------------------------+------------------+
# only showing top 20 rows

analysis.count()
# 1000000

triplets = (spark.read.format("csv").
                    option("header", "false").
                    option("delimiter", "\t").
                    load("/data/msd/tasteprofile/triplets.tsv"))

triplets.show()
# +--------------------+------------------+---+
# |                 _c0|               _c1|_c2|
# +--------------------+------------------+---+
# |f1bfc2a4597a3642f...|SOQEFDN12AB017C52B|  1|
# |f1bfc2a4597a3642f...|SOQOIUJ12A6701DAA7|  2|
# |f1bfc2a4597a3642f...|SOQOKKD12A6701F92E|  4|
# |f1bfc2a4597a3642f...|SOSDVHO12AB01882C7|  1|
# |f1bfc2a4597a3642f...|SOSKICX12A6701F932|  1|
# |f1bfc2a4597a3642f...|SOSNUPV12A8C13939B|  1|
# |f1bfc2a4597a3642f...|SOSVMII12A6701F92D|  1|
# |f1bfc2a4597a3642f...|SOTUNHI12B0B80AFE2|  1|
# |f1bfc2a4597a3642f...|SOTXLTZ12AB017C535|  1|
# |f1bfc2a4597a3642f...|SOTZDDX12A6701F935|  1|
# |f1bfc2a4597a3642f...|SOTZTVF12A58A79B9F|  1|
# |f1bfc2a4597a3642f...|SOUGTZZ12A8C13B8CC|  1|
# |f1bfc2a4597a3642f...|SOVDLVW12A6701F92F|  1|
# |f1bfc2a4597a3642f...|SOVKHBC12AF72A5DE7|  1|
# |f1bfc2a4597a3642f...|SOVKJMM12AF72AAF3C|  1|
# |f1bfc2a4597a3642f...|SOVMWUC12A8C13750B|  1|
# |f1bfc2a4597a3642f...|SOVQPUM12A6D4F8F18|  1|
# |f1bfc2a4597a3642f...|SOVVTJV12AB017B20E|  1|
# |f1bfc2a4597a3642f...|SOXREWC12A6D4FAD3C|  1|
# |f1bfc2a4597a3642f...|SOXTVVG12A6D4F8F16|  1|
# +--------------------+------------------+---+
# only showing top 20 rows

triplets.count()
# 48373586

manually_accepted = spark.read.text("/data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt")

manually_accepted.show()
# +--------------------+
# |               value|
# +--------------------+
# |                 9d8|
# |< ERROR: <SOFQHZM...|
# |               19d17|
# |< ERROR: <SODXUTF...|
# |               29d26|
# |< ERROR: <SOASCRF...|
# |               33d29|
# |< ERROR: <SOITDUN...|
# |               52d47|
# |< ERROR: <SOLZXUM...|
# |             126d120|
# |< ERROR: <SOTJTDT...|
# |             153d146|
# |< ERROR: <SOGCVWB...|
# |             230d222|
# |< ERROR: <SOKDKGD...|
# |             235d226|
# |< ERROR: <SOPPBXP...|
# |             276d266|
# |< ERROR: <SODQSLR...|
# +--------------------+
# only showing top 20 rows


manually_accepted.count()
# 938

mismatches = spark.read.text("/data/msd/tasteprofile/mismatches/sid_mismatches.txt")

mismatches.show()
# +--------------------+
# |               value|
# +--------------------+
# |ERROR: <SOUMNSI12...|
# |ERROR: <SOCMRBE12...|
# |ERROR: <SOLPHZY12...|
# |ERROR: <SONGHTM12...|
# |ERROR: <SONGXCA12...|
# |ERROR: <SOMBCRC12...|
# |ERROR: <SOTDWDK12...|
# |ERROR: <SOEBURP12...|
# |ERROR: <SOSRJHS12...|
# |ERROR: <SOIYAAQ12...|
# |ERROR: <SOUQUOG12...|
# |ERROR: <SORRXFX12...|
# |ERROR: <SOBWQMY12...|
# |ERROR: <SOCYTXQ12...|
# |ERROR: <SOKMSQN12...|
# |ERROR: <SOFXDWK12...|
# |ERROR: <SOBPEIN12...|
# |ERROR: <SOANRSQ12...|
# |ERROR: <SOVWUNG12...|
# |ERROR: <SOOBZPE12...|
# +--------------------+
# only showing top 20 rows


mismatches.count()
# 19094


MAGDgenre = (spark.read.format("csv").
                    option("header", "false").
                    option("delimiter", "\t").
                    load("/data/msd/genre/msd-MAGD-genreAssignment.tsv"))

MAGDgenre.show()
# +------------------+--------------+
# |               _c0|           _c1|
# +------------------+--------------+
# |TRAAAAK128F9318786|      Pop_Rock|
# |TRAAAAV128F421A322|      Pop_Rock|
# |TRAAAAW128F429D538|           Rap|
# |TRAAABD128F429CF47|      Pop_Rock|
# |TRAAACV128F423E09E|      Pop_Rock|
# |TRAAADT12903CCC339|Easy_Listening|
# |TRAAAED128E0783FAB|         Vocal|
# |TRAAAEF128F4273421|      Pop_Rock|
# |TRAAAEM128F93347B9|    Electronic|
# |TRAAAFD128F92F423A|      Pop_Rock|
# |TRAAAFP128F931B4E3|           Rap|
# |TRAAAGR128F425B14B|      Pop_Rock|
# |TRAAAGW12903CC1049|         Blues|
# |TRAAAHD128F42635A5|      Pop_Rock|
# |TRAAAHE12903C9669C|      Pop_Rock|
# |TRAAAHJ128F931194C|      Pop_Rock|
# |TRAAAHZ128E0799171|           Rap|
# |TRAAAIR128F1480971|           RnB|
# |TRAAAJG128F9308A25|          Folk|
# |TRAAAMO128F1481E7F|     Religious|
# +------------------+--------------+
# only showing top 20 rows


MAGDgenre.count()
# 422714

MAGDstyle = (spark.read.format("csv").
                    option("header", "false").
                    option("delimiter", "\t").
                    load("/data/msd/genre/msd-MASD-styleAssignment.tsv"))

MAGDstyle.show()
# +------------------+--------------------+
# |               _c0|                 _c1|
# +------------------+--------------------+
# |TRAAAAK128F9318786|   Metal_Alternative|
# |TRAAAAV128F421A322|                Punk|
# |TRAAAAW128F429D538|         Hip_Hop_Rap|
# |TRAAACV128F423E09E|Rock_Neo_Psychedelia|
# |TRAAAEF128F4273421|           Pop_Indie|
# |TRAAAFP128F931B4E3|         Hip_Hop_Rap|
# |TRAAAGR128F425B14B|    Pop_Contemporary|
# |TRAAAHD128F42635A5|           Rock_Hard|
# |TRAAAHJ128F931194C|           Pop_Indie|
# |TRAAAHZ128E0799171|         Hip_Hop_Rap|
# |TRAAAIR128F1480971|    Pop_Contemporary|
# |TRAAAJG128F9308A25| Country_Traditional|
# |TRAAAMO128F1481E7F|              Gospel|
# |TRAAAMQ128F1460CD3|         Hip_Hop_Rap|
# |TRAAANK128F428B515|Rock_Neo_Psychedelia|
# |TRAAARJ128F9320760|    Pop_Contemporary|
# |TRAAAVO128F93133D4|Rock_Neo_Psychedelia|
# |TRAAAZU128F4226F7A|    Rock_Alternative|
# |TRAABAH128F423B788|           Pop_Indie|
# |TRAABBY128F930C3B5|Rock_Neo_Psychedelia|
# +------------------+--------------------+
# only showing top 20 rows

MAGDstyle.count()
# 273936

topMAGDgenre = (spark.read.format("csv").
                    option("header", "false").
                    option("delimiter", "\t").
                    load("/data/msd/genre/msd-topMAGD-genreAssignment.tsv"))

topMAGDgenre.show()
# +------------------+----------+
# |               _c0|       _c1|
# +------------------+----------+
# |TRAAAAK128F9318786|  Pop_Rock|
# |TRAAAAV128F421A322|  Pop_Rock|
# |TRAAAAW128F429D538|       Rap|
# |TRAAABD128F429CF47|  Pop_Rock|
# |TRAAACV128F423E09E|  Pop_Rock|
# |TRAAAED128E0783FAB|     Vocal|
# |TRAAAEF128F4273421|  Pop_Rock|
# |TRAAAEM128F93347B9|Electronic|
# |TRAAAFD128F92F423A|  Pop_Rock|
# |TRAAAFP128F931B4E3|       Rap|
# |TRAAAGR128F425B14B|  Pop_Rock|
# |TRAAAGW12903CC1049|     Blues|
# |TRAAAHD128F42635A5|  Pop_Rock|
# |TRAAAHE12903C9669C|  Pop_Rock|
# |TRAAAHJ128F931194C|  Pop_Rock|
# |TRAAAHZ128E0799171|       Rap|
# |TRAAAIR128F1480971|       RnB|
# |TRAAAJG128F9308A25|      Folk|
# |TRAAAMQ128F1460CD3|       Rap|
# |TRAAANK128F428B515|  Pop_Rock|
# +------------------+----------+
# only showing top 20 rows


topMAGDgenre.count()
# 406427



sample_properties = spark.read.csv("/data/msd/audio/statistics/sample_properties.csv.gz", header=True)

sample_properties.show()
# +------------------+--------------------+--------------------+---------+----------+--------------+-------------+-----------+-----------+--------------+--------+
# |          track_id|               title|         artist_name| duration|7digita_Id|sample_bitrate|sample_length|sample_rate|sample_mode|sample_version|filesize|
# +------------------+--------------------+--------------------+---------+----------+--------------+-------------+-----------+-----------+--------------+--------+
# |TRMMMYQ128F932D901|        Silent Night|    Faster Pussy cat|252.05506|   7032331|           128|60.1935770567|      22050|          1|             2|  960887|
# |TRMMMKD128F425225D|         Tanssi vaan|    Karkkiautomaatti|156.55138|   1514808|            64|30.2244270016|      22050|          1|             2|  242038|
# |TRMMMRX128F93187D9|   No One Could Ever|      Hudson Mohawke|138.97098|   6945353|           128|60.1935770567|      22050|          1|             2|  960887|
# |TRMMMCH128F425532C|       Si Vos Querés|         Yerba Brava|145.05751|   2168257|            64|30.2083516484|      22050|          1|             2|  240534|
# |TRMMMWA128F426B589|    Tangle Of Aspens|          Der Mystic|514.29832|   2264873|            64|60.3382103611|      22050|          1|             2|  480443|
# |TRMMMXN128F42936A5|"Symphony No. 1 G...|    David Montgomery|816.53506|   3360982|           128|30.1360348456|      44100|          0|             1|  481070|
# |TRMMMLR128F1494097|    We Have Got Love|  Sasha / Turbulence|212.37506|    552626|            64|60.3542857143|      22050|          1|             2|  480686|
# |TRMMMBB12903CB7D21|   2 Da Beat Ch'yall|          Kris Kross|221.20444|   6435649|           128|30.1360348456|      44100|          0|             1|  481070|
# |TRMMMHY12903CB53F1|             Goodbye|        Joseph Locke|139.17995|   8376489|           128|60.2459472422|      22050|          1|             2|  961723|
# |TRMMMML128F4280EE9|Mama_ mama can't ...|The Sun Harbor's ...|104.48934|   1043208|           206|30.0408163265|      44100|          1|             1|  777413|
# |TRMMMNS128F93548E1|       L'antarctique|    3 Gars Su'l Sofa| 68.96281|   7192392|           128|60.2459472422|      22050|          1|             2|  961723|
# |TRMMMXJ12903CBF111|  El hijo del pueblo|       Jorge Negrete|168.22812|   7928975|           128|60.1935770567|      22050|          1|             2|  960887|
# |TRMMMCJ128F930BFF8|Cold Beer feat. P...|        Danny Diablo|301.60934|   6893903|           128|30.1360348456|      44100|          0|             1|  481070|
# |TRMMMBW128F4260CAE|              Pilots|           Tiger Lou|318.45832|   2218694|           128|30.1360348456|      44100|          0|             1|  481070|
# |TRMMMXI128F4285A3F|              N Gana|     Waldemar Bastos|273.18812|   2387740|            64|60.3382103611|      22050|          1|             2|  480443|
# |TRMMMKI128F931D80D|                 006|     Lena Philipsson|262.26893|   6010886|           128|30.1360348456|      44100|          0|             1|  481070|
# |TRMMMUT128F42646E8|(Looking For) The...|        Shawn Colvin|216.47628|   3156269|           128|30.1360348456|      44100|          0|             1|  481070|
# |TRMMMQY128F92F0EA3|   Ethos of Coercion|         Dying Fetus| 196.0224|   6782293|           128|60.2459472422|      22050|          1|             2|  961723|
# |TRMMMTK128F424EF7C|         Rock-N-Rule|               Emery|217.57342|   1501464|            64|30.2244270016|      22050|          1|             2|  242073|
# |TRMMMHQ128F4278194|       La bola extra|        Los Ronaldos|355.60444|   2318206|            64| 30.234599686|      22050|          1|             2|  240743|
# +------------------+--------------------+--------------------+---------+----------+--------------+-------------+-----------+-----------+--------------+--------+
# only showing top 20 rows


sample_properties.count()
# 992865

msd_jmir_ar = spark.read.csv("/data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv")

msd_jmir_ar.count()
# 994623

msd_jmir_lp = spark.read.csv("/data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv")

msd_jmir_lp.count()
# 994623

msd_jmir_met = spark.read.csv("/data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv")

msd_jmir_met.count()
# 994623

msd_jmir_mfcc = spark.read.csv("/data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv")

msd_jmir_mfcc.count()
# 994623

msd_jmir_spec = spark.read.csv("/data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv")

msd_jmir_spec.count()
# 994623

msd_jmir_spec_der = spark.read.csv("/data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv")

msd_jmir_spec_der.count()
# 994623

msd_mar = spark.read.csv("/data/msd/audio/features/msd-marsyas-timbral-v1.0.csv")

msd_mar.count()
# 995001


msd_mvd = spark.read.csv("/data/msd/audio/features/msd-mvd-v1.0.csv")

msd_mvd.count()
# 994188


msd_rh = spark.read.csv("/data/msd/audio/features/msd-rh-v1.0.csv")

msd_rh.count()
# 994188

msd_rp = spark.read.csv("/data/msd/audio/features/msd-rp-v1.0.csv")

msd_rp.count()
# 994188

msd_ssd = spark.read.csv("/data/msd/audio/features/msd-ssd-v1.0.csv")

msd_ssd.count()
# 994188

msd_trh = spark.read.csv("/data/msd/audio/features/msd-trh-v1.0.csv")

msd_trh.count()
# 994188

msd_tssd = spark.read.csv("/data/msd/audio/features/msd-tssd-v1.0.csv")

msd_tssd.count()
# 994188






#QUESTION 2a

#the following code is written by James Williams
mismatches_schema = StructType([
  StructField("song_id", StringType(), True),
  StructField("song_artist", StringType(), True),
  StructField("song_title", StringType(), True),
  StructField("track_id", StringType(), True),
  StructField("track_artist", StringType(), True),
  StructField("track_title", StringType(), True)
])

with open("/scratch-network/courses/2022/DATA420-22S1/data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt", "r") as f:
  lines = f.readlines()
  sid_matches_manually_accepted = []
  for line in lines:
    if line.startswith("< ERROR: "):
      a = line[10:28]
      b = line[29:47]
      c, d = line[49:-1].split("  !=  ")
      e, f = c.split("  -  ")
      g, h = d.split("  -  ")
      sid_matches_manually_accepted.append((a, e, f, b, g, h))

matches_manually_accepted = spark.createDataFrame(sc.parallelize(sid_matches_manually_accepted, 8), schema=mismatches_schema)

matches_manually_accepted.show()
# +------------------+--------------------+--------------------+------------------+--------------------+--------------------+
# |           song_id|         song_artist|          song_title|          track_id|        track_artist|         track_title|
# +------------------+--------------------+--------------------+------------------+--------------------+--------------------+
# |SOFQHZM12A8C142342|        Josipa Lisac|             razloga|TRMWMFG128F92FFEF2|        Lisac Josipa|        1000 razloga|
# |SODXUTF12AB018A3DA|          Lutan Fyah|Nuh Matter the Cr...|TRMWPCD12903CCE5ED|             Midnite|Nah Matter the Cr...|
# |SOASCRF12A8C1372E6|   Gaetano Donizetti|L'Elisir d'Amore:...|TRMHIPJ128F426A2E2|Gianandrea Gavazz...|L'Elisir D'Amore_...|
# |SOITDUN12A58A7AACA|        C.J. Chenier|           Ay, Ai Ai|TRMHXGK128F42446AB|     Clifton Chenier|           Ay_ Ai Ai|
# |SOLZXUM12AB018BE39|               許志安|              男人最痛|TRMRSOF12903CCF516|            Andy Hui|    Nan Ren Zui Tong|
# |SOTJTDT12A8C13A8A6|                   S|                   h|TRMNKQE128F427C4D8|         Sammy Hagar|20th Century Man ...|
# |SOGCVWB12AB0184CE2|                   H|                   Y|TRMUNCZ128F932A95D|            Hawkwind|25 Years (Alterna...|
# |SOKDKGD12AB0185E9C|          影山ヒロノブ|Cha-La Head-Cha-L...|TRMOOAH12903CB4B29|    Takahashi Hiroki|Maka fushigi adve...|
# |SOPPBXP12A8C141194|       Αντώνης Ρέμος|    O Trellos - Live|TRMXJDS128F42AE7CF|       Antonis Remos|           O Trellos|
# |SODQSLR12A8C133A01|       John Williams|Concerto No. 1 fo...|TRWHMXN128F426E03C|English Chamber O...|II. Andantino sic...|
# |SOFHEGK12A8C130C4B|                   V|                   W|TRWCMGK128F423DE25|       Velocity Girl|    57 Waltz (Album)|
# |SODJDRW12AB018153D|               陳奕迅|     Da Ge Nu - Live|TRWRUJE128F92F354C|          Eason Chan|            Da Ge Nu|
# |SOWXIFY12A6D4F80AD|Angèle Dubeau & L...|Sinfonia per arch...|TRWBDOK128EF364A68|Tafelmusik & Matt...|Sinfonia Per Arch...|
# |SOBRCIA12A8C134025|              Larsen|Vas Leur Dire (Fe...|TRWNFEE128F426BE5A|    Soprano - Larsen|        Va Leur Dire|
# |SOTWJND12AB01864B0|       Aaron Neville|  Sweet Little Alice|TRWPQJV128F9339510|Aaron and Art Nev...|Hey Little Alice ...|
# |SOUDVBF12AB018A14A|    Γιώργος Τσαλίκης|       Ourane - Live|TRWTEDC12903CAA171|    Giorgos Tsalikis|              Ourane|
# |SOFNPTF12A8C13F39E|                   S|                   V|TRWLPQJ128F4284284|             Solomon|25 Variations And...|
# |SORKCYV12A8C136055|   Gaetano Donizetti|L'Elisir d'Amore:...|TRWEWQE128F426A2CB|Gianandrea Gavazz...|L'Elisir D'Amore_...|
# |SORIOPF12A58A7CF4D|Israel Philharmon...|Autumn: Allegro (...|TRWSHIN128F931E1F3|  I Virtuosi Di Roma|             Allegro|
# |SORXFLW12AC9072548|Tbilisi Symphony ...|The Nutcracker, O...|TRWSOYY12903CF6006|Swan Lake & The N...|No.14. Pas de deu...|
# +------------------+--------------------+--------------------+------------------+--------------------+--------------------+
# only showing top 20 rows


print(matches_manually_accepted.count())
# 488


with open("/scratch-network/courses/2022/DATA420-22S1/data/msd/tasteprofile/mismatches/sid_mismatches.txt", "r") as f:
  lines = f.readlines()
  sid_mismatches = []
  for line in lines:
    if line.startswith("ERROR: "):
      a = line[8:26]
      b = line[27:45]
      c, d = line[47:-1].split("  !=  ")
      e, f = c.split("  -  ")
      g, h = d.split("  -  ")
      sid_mismatches.append((a, e, f, b, g, h))

mismatches = spark.createDataFrame(sc.parallelize(sid_mismatches, 64), schema=mismatches_schema)

mismatches.show()
# +------------------+--------------------+--------------------+------------------+--------------------+--------------------+
# |           song_id|         song_artist|          song_title|          track_id|        track_artist|         track_title|
# +------------------+--------------------+--------------------+------------------+--------------------+--------------------+
# |SOUMNSI12AB0182807| Digital Underground|    The Way We Swing|TRMMGKQ128F9325E10|            Linkwood|Whats up with the...|
# |SOCMRBE12AB018C546|          Jimmy Reed|The Sun Is Shinin...|TRMMREB12903CEB1B1|          Slim Harpo|I Got Love If You...|
# |SOLPHZY12AC468ABA8|       Africa HiTech|            Footstep|TRMMBOC12903CEB46E|      Marcus Worgull|Drumstern (BONUS ...|
# |SONGHTM12A8C1374EF|      Death in Vegas|        Anita Berber|TRMMITP128F425D8D0|           Valen Hsu|              Shi Yi|
# |SONGXCA12A8C13E82E|  Grupo Exterminador|       El Triunfador|TRMMAYZ128F429ECE6|           I Ribelli|           Lei M'Ama|
# |SOMBCRC12A67ADA435|       Fading Friend|         Get us out!|TRMMNVU128EF343EED|           Masterboy|  Feel The Heat 2000|
# |SOTDWDK12A8C13617B|        Daevid Allen|          Past Lives|TRMMNCZ128F426FF0E|       Bhimsen Joshi|Raga - Shuddha Sa...|
# |SOEBURP12AB018C2FB|   Cristian Paduraru|          Born Again|TRMMPBS12903CE90E1|           Yespiring|      Journey Stages|
# |SOSRJHS12A6D4FDAA3|          Jeff Mills|  Basic Human Design|TRMWMEL128F421DA68|                 M&T|       Drumsettester|
# |SOIYAAQ12A6D4F954A|            Excepter|                  OG|TRMWHRI128F147EA8E|          The Fevers|Não Tenho Nada (N...|
# |SOUQUOG12A6D4F5965|The Amalgamation ...|                  31|TRMWCQW128F147DE05|       Yvonne Loriod|Messiaen : 8 Prél...|
# |SORRXFX12A8C141030|         A.R. Rahman|Chaiyya Chaiyya (...|TRMWRQM128F92CCC1C|Vishal|A.R.Rehman...| Maro Chauke Chhakke|
# |SOBWQMY12AB018AEAD|               Addex|We Fall So Deep (...|TRMWBQO128F935220C|      Julien creance|           One world|
# |SOCYTXQ12AB018E7C9|      Technical Itch|  The Way Things Are|TRMWQBZ12903CC6AEF|  Hixxy / Technikore|           Re-Loaded|
# |SOKMSQN12A6D4FA08E|Sir Neville Marri...|           Träumerei|TRMWQNL128F92C6A25|     Evelyne Dubourg|Robert Schumann: ...|
# |SOFXDWK12A58A77B03|        The Alliance|           Feel This|TRMWAQB128F428764C|  Hackberry Ramblers|        Step It Fast|
# |SOBPEIN12AB018113A|       Late Bloomers|Money Maker (Inst...|TRMWPYJ128F931C084|100 Proof Aged in...|Im Mad As Hell (...|
# |SOANRSQ12A81C2147D|     My Robot Friend|America Is Automa...|TRMWDXD128F42554E8|       Baggi Begovic|       Break Of Dawn|
# |SOVWUNG12A8C137891|             Warlock|      Copy of a Copy|TRMGMLW128F426A200|             Sickboy|    She's out of way|
# |SOOBZPE12AC468DFAF|    Sabor Merenguero|Me Tiene Loco-Chi...|TRMGWMD12903D03949|           Chico Che|        Me Tine Loco|
# +------------------+--------------------+--------------------+------------------+--------------------+--------------------+
# only showing top 20 rows




print(mismatches.count())
# 19094

triplets_schema = StructType([
  StructField("user_id", StringType(), True),
  StructField("song_id", StringType(), True),
  StructField("plays", IntegerType(), True)
])
triplets = (
  spark.read.format("csv")
  .option("header", "false")
  .option("delimiter", "\t")
  .option("codec", "gzip")
  .schema(triplets_schema)
  .load("hdfs:///data/msd/tasteprofile/triplets.tsv/")
  .cache()
)


triplets.show()
# +--------------------+------------------+-----+
# |             user_id|           song_id|plays|
# +--------------------+------------------+-----+
# |f1bfc2a4597a3642f...|SOQEFDN12AB017C52B|    1|
# |f1bfc2a4597a3642f...|SOQOIUJ12A6701DAA7|    2|
# |f1bfc2a4597a3642f...|SOQOKKD12A6701F92E|    4|
# |f1bfc2a4597a3642f...|SOSDVHO12AB01882C7|    1|
# |f1bfc2a4597a3642f...|SOSKICX12A6701F932|    1|
# |f1bfc2a4597a3642f...|SOSNUPV12A8C13939B|    1|
# |f1bfc2a4597a3642f...|SOSVMII12A6701F92D|    1|
# |f1bfc2a4597a3642f...|SOTUNHI12B0B80AFE2|    1|
# |f1bfc2a4597a3642f...|SOTXLTZ12AB017C535|    1|
# |f1bfc2a4597a3642f...|SOTZDDX12A6701F935|    1|
# |f1bfc2a4597a3642f...|SOTZTVF12A58A79B9F|    1|
# |f1bfc2a4597a3642f...|SOUGTZZ12A8C13B8CC|    1|
# |f1bfc2a4597a3642f...|SOVDLVW12A6701F92F|    1|
# |f1bfc2a4597a3642f...|SOVKHBC12AF72A5DE7|    1|
# |f1bfc2a4597a3642f...|SOVKJMM12AF72AAF3C|    1|
# |f1bfc2a4597a3642f...|SOVMWUC12A8C13750B|    1|
# |f1bfc2a4597a3642f...|SOVQPUM12A6D4F8F18|    1|
# |f1bfc2a4597a3642f...|SOVVTJV12AB017B20E|    1|
# |f1bfc2a4597a3642f...|SOXREWC12A6D4FAD3C|    1|
# |f1bfc2a4597a3642f...|SOXTVVG12A6D4F8F16|    1|
# +--------------------+------------------+-----+
# only showing top 20 rows


mismatches_not_accepted = mismatches.join(matches_manually_accepted, on="song_id", how="left_anti")
triplets_not_mismatched = triplets.join(mismatches_not_accepted, on="song_id", how="left_anti")

print(triplets.count())
# 48373586

print(triplets_not_mismatched.count())
# 45795111




#QUESTION 2b

#the following code is written by James Williams
audio_attribute_type_mapping = {
    "NUMERIC": DoubleType(),
    "real": DoubleType(),
    "string": StringType(),
    "STRING": StringType()
}

audio_dataset_names = [
    "msd-jmir-area-of-moments-all-v1.0",
    "msd-jmir-lpc-all-v1.0",
    "msd-jmir-methods-of-moments-all-v1.0",
    "msd-jmir-mfcc-all-v1.0",
    "msd-jmir-spectral-all-all-v1.0",
    "msd-jmir-spectral-derivatives-all-all-v1.0",
    "msd-marsyas-timbral-v1.0",
    "msd-mvd-v1.0",
    "msd-rh-v1.0",
    "msd-rp-v1.0",
    "msd-ssd-v1.0",
    "msd-trh-v1.0",
    "msd-tssd-v1.0"
]

audio_dataset_schemas = {}
for audio_dataset_name in audio_dataset_names:

    audio_dataset_path = f"/scratch-network/courses/2022/DATA420-22S1/data/msd/audio/attributes/{audio_dataset_name}.attributes.csv"
    with open(audio_dataset_path, "r") as f:
        rows = [line.strip().split(",") for line in f.readlines()]

    audio_dataset_schemas[audio_dataset_name] = StructType([
        StructField(row[0], audio_attribute_type_mapping[row[1]], True) for row in rows
    ])

    print(audio_dataset_schemas[audio_dataset_name])


# StructType(List(StructField(Area_Method_of_Moments_Overall_Standard_Deviation_1,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Standard_Deviation_2,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Standard_Deviation_3,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Standard_Deviation_4,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Standard_Deviation_5,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Standard_Deviation_6,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Standard_Deviation_7,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Standard_Deviation_8,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Standard_Deviation_9,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Standard_Deviation_10,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Average_1,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Average_2,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Average_3,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Average_4,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Average_5,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Average_6,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Average_7,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Average_8,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Average_9,DoubleType,true),StructField(Area_Method_of_Moments_Overall_Average_10,DoubleType,true),StructField(MSD_TRACKID,StringType,true)))
# StructType(List(StructField(LPC_Overall_Standard_Deviation_1,DoubleType,true),StructField(LPC_Overall_Standard_Deviation_2,DoubleType,true),StructField(LPC_Overall_Standard_Deviation_3,DoubleType,true),StructField(LPC_Overall_Standard_Deviation_4,DoubleType,true),StructField(LPC_Overall_Standard_Deviation_5,DoubleType,true),StructField(LPC_Overall_Standard_Deviation_6,DoubleType,true),StructField(LPC_Overall_Standard_Deviation_7,DoubleType,true),StructField(LPC_Overall_Standard_Deviation_8,DoubleType,true),StructField(LPC_Overall_Standard_Deviation_9,DoubleType,true),StructField(LPC_Overall_Standard_Deviation_10,DoubleType,true),StructField(LPC_Overall_Average_1,DoubleType,true),StructField(LPC_Overall_Average_2,DoubleType,true),StructField(LPC_Overall_Average_3,DoubleType,true),StructField(LPC_Overall_Average_4,DoubleType,true),StructField(LPC_Overall_Average_5,DoubleType,true),StructField(LPC_Overall_Average_6,DoubleType,true),StructField(LPC_Overall_Average_7,DoubleType,true),StructField(LPC_Overall_Average_8,DoubleType,true),StructField(LPC_Overall_Average_9,DoubleType,true),StructField(LPC_Overall_Average_10,DoubleType,true),StructField(MSD_TRACKID,StringType,true)))
# StructType(List(StructField(Method_of_Moments_Overall_Standard_Deviation_1,DoubleType,true),StructField(Method_of_Moments_Overall_Standard_Deviation_2,DoubleType,true),StructField(Method_of_Moments_Overall_Standard_Deviation_3,DoubleType,true),StructField(Method_of_Moments_Overall_Standard_Deviation_4,DoubleType,true),StructField(Method_of_Moments_Overall_Standard_Deviation_5,DoubleType,true),StructField(Method_of_Moments_Overall_Average_1,DoubleType,true),StructField(Method_of_Moments_Overall_Average_2,DoubleType,true),StructField(Method_of_Moments_Overall_Average_3,DoubleType,true),StructField(Method_of_Moments_Overall_Average_4,DoubleType,true),StructField(Method_of_Moments_Overall_Average_5,DoubleType,true),StructField(MSD_TRACKID,StringType,true)))
# StructType(List(StructField(MFCC_Overall_Standard_Deviation_1,DoubleType,true),StructField(MFCC_Overall_Standard_Deviation_2,DoubleType,true),StructField(MFCC_Overall_Standard_Deviation_3,DoubleType,true),StructField(MFCC_Overall_Standard_Deviation_4,DoubleType,true),StructField(MFCC_Overall_Standard_Deviation_5,DoubleType,true),StructField(MFCC_Overall_Standard_Deviation_6,DoubleType,true),StructField(MFCC_Overall_Standard_Deviation_7,DoubleType,true),StructField(MFCC_Overall_Standard_Deviation_8,DoubleType,true),StructField(MFCC_Overall_Standard_Deviation_9,DoubleType,true),StructField(MFCC_Overall_Standard_Deviation_10,DoubleType,true),StructField(MFCC_Overall_Standard_Deviation_11,DoubleType,true),StructField(MFCC_Overall_Standard_Deviation_12,DoubleType,true),StructField(MFCC_Overall_Standard_Deviation_13,DoubleType,true),StructField(MFCC_Overall_Average_1,DoubleType,true),StructField(MFCC_Overall_Average_2,DoubleType,true),StructField(MFCC_Overall_Average_3,DoubleType,true),StructField(MFCC_Overall_Average_4,DoubleType,true),StructField(MFCC_Overall_Average_5,DoubleType,true),StructField(MFCC_Overall_Average_6,DoubleType,true),StructField(MFCC_Overall_Average_7,DoubleType,true),StructField(MFCC_Overall_Average_8,DoubleType,true),StructField(MFCC_Overall_Average_9,DoubleType,true),StructField(MFCC_Overall_Average_10,DoubleType,true),StructField(MFCC_Overall_Average_11,DoubleType,true),StructField(MFCC_Overall_Average_12,DoubleType,true),StructField(MFCC_Overall_Average_13,DoubleType,true),StructField(MSD_TRACKID,StringType,true)))
# StructType(List(StructField(Spectral_Centroid_Overall_Standard_Deviation_1,DoubleType,true),StructField(Spectral_Rolloff_Point_Overall_Standard_Deviation_1,DoubleType,true),StructField(Spectral_Flux_Overall_Standard_Deviation_1,DoubleType,true),StructField(Compactness_Overall_Standard_Deviation_1,DoubleType,true),StructField(Spectral_Variability_Overall_Standard_Deviation_1,DoubleType,true),StructField(Root_Mean_Square_Overall_Standard_Deviation_1,DoubleType,true),StructField(Fraction_Of_Low_Energy_Windows_Overall_Standard_Deviation_1,DoubleType,true),StructField(Zero_Crossings_Overall_Standard_Deviation_1,DoubleType,true),StructField(Spectral_Centroid_Overall_Average_1,DoubleType,true),StructField(Spectral_Rolloff_Point_Overall_Average_1,DoubleType,true),StructField(Spectral_Flux_Overall_Average_1,DoubleType,true),StructField(Compactness_Overall_Average_1,DoubleType,true),StructField(Spectral_Variability_Overall_Average_1,DoubleType,true),StructField(Root_Mean_Square_Overall_Average_1,DoubleType,true),StructField(Fraction_Of_Low_Energy_Windows_Overall_Average_1,DoubleType,true),StructField(Zero_Crossings_Overall_Average_1,DoubleType,true),StructField(MSD_TRACKID,StringType,true)))
# StructType(List(StructField(Spectral_Centroid_Overall_Standard_Deviation_1,DoubleType,true),StructField(Spectral_Rolloff_Point_Overall_Standard_Deviation_1,DoubleType,true),StructField(Spectral_Flux_Overall_Standard_Deviation_1,DoubleType,true),StructField(Compactness_Overall_Standard_Deviation_1,DoubleType,true),StructField(Spectral_Variability_Overall_Standard_Deviation_1,DoubleType,true),StructField(Root_Mean_Square_Overall_Standard_Deviation_1,DoubleType,true),StructField(Fraction_Of_Low_Energy_Windows_Overall_Standard_Deviation_1,DoubleType,true),StructField(Zero_Crossings_Overall_Standard_Deviation_1,DoubleType,true),StructField(Spectral_Centroid_Overall_Average_1,DoubleType,true),StructField(Spectral_Rolloff_Point_Overall_Average_1,DoubleType,true),StructField(Spectral_Flux_Overall_Average_1,DoubleType,true),StructField(Compactness_Overall_Average_1,DoubleType,true),StructField(Spectral_Variability_Overall_Average_1,DoubleType,true),StructField(Root_Mean_Square_Overall_Average_1,DoubleType,true),StructField(Fraction_Of_Low_Energy_Windows_Overall_Average_1,DoubleType,true),StructField(Zero_Crossings_Overall_Average_1,DoubleType,true),StructField(MSD_TRACKID,StringType,true)))
# StructType(List(StructField(Mean_Acc5_Mean_Mem20_ZeroCrossings_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_Centroid_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_Rolloff_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_Flux_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_MFCC0_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_MFCC1_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_MFCC2_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_MFCC3_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_MFCC4_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_MFCC5_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_MFCC6_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_MFCC7_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_MFCC8_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_MFCC9_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_MFCC10_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_MFCC11_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_MFCC12_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_PeakRatio_Chroma_A_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_PeakRatio_Chroma_A#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_PeakRatio_Chroma_B_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_PeakRatio_Chroma_C_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_PeakRatio_Chroma_C#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_PeakRatio_Chroma_D_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_PeakRatio_Chroma_D#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_PeakRatio_Chroma_E_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_PeakRatio_Chroma_F_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_PeakRatio_Chroma_F#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_PeakRatio_Chroma_G_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_PeakRatio_Chroma_G#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_PeakRatio_Average_Chroma_A_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Mean_Mem20_PeakRatio_Minimum_Chroma_A_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_ZeroCrossings_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_Centroid_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_Rolloff_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_Flux_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_MFCC0_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_MFCC1_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_MFCC2_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_MFCC3_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_MFCC4_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_MFCC5_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_MFCC6_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_MFCC7_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_MFCC8_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_MFCC9_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_MFCC10_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_MFCC11_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_MFCC12_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_PeakRatio_Chroma_A_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_PeakRatio_Chroma_A#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_PeakRatio_Chroma_B_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_PeakRatio_Chroma_C_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_PeakRatio_Chroma_C#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_PeakRatio_Chroma_D_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_PeakRatio_Chroma_D#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_PeakRatio_Chroma_E_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_PeakRatio_Chroma_F_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_PeakRatio_Chroma_F#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_PeakRatio_Chroma_G_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_PeakRatio_Chroma_G#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_PeakRatio_Average_Chroma_A_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Mean_Acc5_Std_Mem20_PeakRatio_Minimum_Chroma_A_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_ZeroCrossings_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_Centroid_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_Rolloff_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_Flux_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_MFCC0_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_MFCC1_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_MFCC2_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_MFCC3_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_MFCC4_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_MFCC5_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_MFCC6_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_MFCC7_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_MFCC8_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_MFCC9_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_MFCC10_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_MFCC11_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_MFCC12_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_PeakRatio_Chroma_A_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_PeakRatio_Chroma_A#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_PeakRatio_Chroma_B_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_PeakRatio_Chroma_C_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_PeakRatio_Chroma_C#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_PeakRatio_Chroma_D_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_PeakRatio_Chroma_D#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_PeakRatio_Chroma_E_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_PeakRatio_Chroma_F_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_PeakRatio_Chroma_F#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_PeakRatio_Chroma_G_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_PeakRatio_Chroma_G#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_PeakRatio_Average_Chroma_A_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Mean_Mem20_PeakRatio_Minimum_Chroma_A_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_ZeroCrossings_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_Centroid_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_Rolloff_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_Flux_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_MFCC0_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_MFCC1_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_MFCC2_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_MFCC3_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_MFCC4_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_MFCC5_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_MFCC6_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_MFCC7_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_MFCC8_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_MFCC9_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_MFCC10_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_MFCC11_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_MFCC12_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_PeakRatio_Chroma_A_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_PeakRatio_Chroma_A#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_PeakRatio_Chroma_B_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_PeakRatio_Chroma_C_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_PeakRatio_Chroma_C#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_PeakRatio_Chroma_D_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_PeakRatio_Chroma_D#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_PeakRatio_Chroma_E_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_PeakRatio_Chroma_F_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_PeakRatio_Chroma_F#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_PeakRatio_Chroma_G_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_PeakRatio_Chroma_G#_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_PeakRatio_Average_Chroma_A_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(Std_Acc5_Std_Mem20_PeakRatio_Minimum_Chroma_A_Power_powerFFT_WinHamming_HopSize512_WinSize512_Sum_AudioCh0,DoubleType,true),StructField(track_id,StringType,true)))
# StructType(List(StructField("component_0",DoubleType,true),StructField("component_1",DoubleType,true),StructField("component_2",DoubleType,true),StructField("component_3",DoubleType,true),StructField("component_4",DoubleType,true),StructField("component_5",DoubleType,true),StructField("component_6",DoubleType,true),StructField("component_7",DoubleType,true),StructField("component_8",DoubleType,true),StructField("component_9",DoubleType,true),StructField("component_10",DoubleType,true),StructField("component_11",DoubleType,true),StructField("component_12",DoubleType,true),StructField("component_13",DoubleType,true),StructField("component_14",DoubleType,true),StructField("component_15",DoubleType,true),StructField("component_16",DoubleType,true),StructField("component_17",DoubleType,true),StructField("component_18",DoubleType,true),StructField("component_19",DoubleType,true),StructField("component_20",DoubleType,true),StructField("component_21",DoubleType,true),StructField("component_22",DoubleType,true),StructField("component_23",DoubleType,true),StructField("component_24",DoubleType,true),StructField("component_25",DoubleType,true),StructField("component_26",DoubleType,true),StructField("component_27",DoubleType,true),StructField("component_28",DoubleType,true),StructField("component_29",DoubleType,true),StructField("component_30",DoubleType,true),StructField("component_31",DoubleType,true),StructField("component_32",DoubleType,true),StructField("component_33",DoubleType,true),StructField("component_34",DoubleType,true),StructField("component_35",DoubleType,true),StructField("component_36",DoubleType,true),StructField("component_37",DoubleType,true),StructField("component_38",DoubleType,true),StructField("component_39",DoubleType,true),StructField("component_40",DoubleType,true),StructField("component_41",DoubleType,true),StructField("component_42",DoubleType,true),StructField("component_43",DoubleType,true),StructField("component_44",DoubleType,true),StructField("component_45",DoubleType,true),StructField("component_46",DoubleType,true),StructField("component_47",DoubleType,true),StructField("component_48",DoubleType,true),StructField("component_49",DoubleType,true),StructField("component_50",DoubleType,true),StructField("component_51",DoubleType,true),StructField("component_52",DoubleType,true),StructField("component_53",DoubleType,true),StructField("component_54",DoubleType,true),StructField("component_55",DoubleType,true),StructField("component_56",DoubleType,true),StructField("component_57",DoubleType,true),StructField("component_58",DoubleType,true),StructField("component_59",DoubleType,true),StructField("component_60",DoubleType,true),StructField("component_61",DoubleType,true),StructField("component_62",DoubleType,true),StructField("component_63",DoubleType,true),StructField("component_64",DoubleType,true),StructField("component_65",DoubleType,true),StructField("component_66",DoubleType,true),StructField("component_67",DoubleType,true),StructField("component_68",DoubleType,true),StructField("component_69",DoubleType,true),StructField("component_70",DoubleType,true),StructField("component_71",DoubleType,true),StructField("component_72",DoubleType,true),StructField("component_73",DoubleType,true),StructField("component_74",DoubleType,true),StructField("component_75",DoubleType,true),StructField("component_76",DoubleType,true),StructField("component_77",DoubleType,true),StructField("component_78",DoubleType,true),StructField("component_79",DoubleType,true),StructField("component_80",DoubleType,true),StructField("component_81",DoubleType,true),StructField("component_82",DoubleType,true),StructField("component_83",DoubleType,true),StructField("component_84",DoubleType,true),StructField("component_85",DoubleType,true),StructField("component_86",DoubleType,true),StructField("component_87",DoubleType,true),StructField("component_88",DoubleType,true),StructField("component_89",DoubleType,true),StructField("component_90",DoubleType,true),StructField("component_91",DoubleType,true),StructField("component_92",DoubleType,true),StructField("component_93",DoubleType,true),StructField("component_94",DoubleType,true),StructField("component_95",DoubleType,true),StructField("component_96",DoubleType,true),StructField("component_97",DoubleType,true),StructField("component_98",DoubleType,true),StructField("component_99",DoubleType,true),StructField("component_100",DoubleType,true),StructField("component_101",DoubleType,true),StructField("component_102",DoubleType,true),StructField("component_103",DoubleType,true),StructField("component_104",DoubleType,true),StructField("component_105",DoubleType,true),StructField("component_106",DoubleType,true),StructField("component_107",DoubleType,true),StructField("component_108",DoubleType,true),StructField("component_109",DoubleType,true),StructField("component_110",DoubleType,true),StructField("component_111",DoubleType,true),StructField("component_112",DoubleType,true),StructField("component_113",DoubleType,true),StructField("component_114",DoubleType,true),StructField("component_115",DoubleType,true),StructField("component_116",DoubleType,true),StructField("component_117",DoubleType,true),StructField("component_118",DoubleType,true),StructField("component_119",DoubleType,true),StructField("component_120",DoubleType,true),StructField("component_121",DoubleType,true),StructField("component_122",DoubleType,true),StructField("component_123",DoubleType,true),StructField("component_124",DoubleType,true),StructField("component_125",DoubleType,true),StructField("component_126",DoubleType,true),StructField("component_127",DoubleType,true),StructField("component_128",DoubleType,true),StructField("component_129",DoubleType,true),StructField("component_130",DoubleType,true),StructField("component_131",DoubleType,true),StructField("component_132",DoubleType,true),StructField("component_133",DoubleType,true),StructField("component_134",DoubleType,true),StructField("component_135",DoubleType,true),StructField("component_136",DoubleType,true),StructField("component_137",DoubleType,true),StructField("component_138",DoubleType,true),StructField("component_139",DoubleType,true),StructField("component_140",DoubleType,true),StructField("component_141",DoubleType,true),StructField("component_142",DoubleType,true),StructField("component_143",DoubleType,true),StructField("component_144",DoubleType,true),StructField("component_145",DoubleType,true),StructField("component_146",DoubleType,true),StructField("component_147",DoubleType,true),StructField("component_148",DoubleType,true),StructField("component_149",DoubleType,true),StructField("component_150",DoubleType,true),StructField("component_151",DoubleType,true),StructField("component_152",DoubleType,true),StructField("component_153",DoubleType,true),StructField("component_154",DoubleType,true),StructField("component_155",DoubleType,true),StructField("component_156",DoubleType,true),StructField("component_157",DoubleType,true),StructField("component_158",DoubleType,true),StructField("component_159",DoubleType,true),StructField("component_160",DoubleType,true),StructField("component_161",DoubleType,true),StructField("component_162",DoubleType,true),StructField("component_163",DoubleType,true),StructField("component_164",DoubleType,true),StructField("component_165",DoubleType,true),StructField("component_166",DoubleType,true),StructField("component_167",DoubleType,true),StructField("component_168",DoubleType,true),StructField("component_169",DoubleType,true),StructField("component_170",DoubleType,true),StructField("component_171",DoubleType,true),StructField("component_172",DoubleType,true),StructField("component_173",DoubleType,true),StructField("component_174",DoubleType,true),StructField("component_175",DoubleType,true),StructField("component_176",DoubleType,true),StructField("component_177",DoubleType,true),StructField("component_178",DoubleType,true),StructField("component_179",DoubleType,true),StructField("component_180",DoubleType,true),StructField("component_181",DoubleType,true),StructField("component_182",DoubleType,true),StructField("component_183",DoubleType,true),StructField("component_184",DoubleType,true),StructField("component_185",DoubleType,true),StructField("component_186",DoubleType,true),StructField("component_187",DoubleType,true),StructField("component_188",DoubleType,true),StructField("component_189",DoubleType,true),StructField("component_190",DoubleType,true),StructField("component_191",DoubleType,true),StructField("component_192",DoubleType,true),StructField("component_193",DoubleType,true),StructField("component_194",DoubleType,true),StructField("component_195",DoubleType,true),StructField("component_196",DoubleType,true),StructField("component_197",DoubleType,true),StructField("component_198",DoubleType,true),StructField("component_199",DoubleType,true),StructField("component_200",DoubleType,true),StructField("component_201",DoubleType,true),StructField("component_202",DoubleType,true),StructField("component_203",DoubleType,true),StructField("component_204",DoubleType,true),StructField("component_205",DoubleType,true),StructField("component_206",DoubleType,true),StructField("component_207",DoubleType,true),StructField("component_208",DoubleType,true),StructField("component_209",DoubleType,true),StructField("component_210",DoubleType,true),StructField("component_211",DoubleType,true),StructField("component_212",DoubleType,true),StructField("component_213",DoubleType,true),StructField("component_214",DoubleType,true),StructField("component_215",DoubleType,true),StructField("component_216",DoubleType,true),StructField("component_217",DoubleType,true),StructField("component_218",DoubleType,true),StructField("component_219",DoubleType,true),StructField("component_220",DoubleType,true),StructField("component_221",DoubleType,true),StructField("component_222",DoubleType,true),StructField("component_223",DoubleType,true),StructField("component_224",DoubleType,true),StructField("component_225",DoubleType,true),StructField("component_226",DoubleType,true),StructField("component_227",DoubleType,true),StructField("component_228",DoubleType,true),StructField("component_229",DoubleType,true),StructField("component_230",DoubleType,true),StructField("component_231",DoubleType,true),StructField("component_232",DoubleType,true),StructField("component_233",DoubleType,true),StructField("component_234",DoubleType,true),StructField("component_235",DoubleType,true),StructField("component_236",DoubleType,true),StructField("component_237",DoubleType,true),StructField("component_238",DoubleType,true),StructField("component_239",DoubleType,true),StructField("component_240",DoubleType,true),StructField("component_241",DoubleType,true),StructField("component_242",DoubleType,true),StructField("component_243",DoubleType,true),StructField("component_244",DoubleType,true),StructField("component_245",DoubleType,true),StructField("component_246",DoubleType,true),StructField("component_247",DoubleType,true),StructField("component_248",DoubleType,true),StructField("component_249",DoubleType,true),StructField("component_250",DoubleType,true),StructField("component_251",DoubleType,true),StructField("component_252",DoubleType,true),StructField("component_253",DoubleType,true),StructField("component_254",DoubleType,true),StructField("component_255",DoubleType,true),StructField("component_256",DoubleType,true),StructField("component_257",DoubleType,true),StructField("component_258",DoubleType,true),StructField("component_259",DoubleType,true),StructField("component_260",DoubleType,true),StructField("component_261",DoubleType,true),StructField("component_262",DoubleType,true),StructField("component_263",DoubleType,true),StructField("component_264",DoubleType,true),StructField("component_265",DoubleType,true),StructField("component_266",DoubleType,true),StructField("component_267",DoubleType,true),StructField("component_268",DoubleType,true),StructField("component_269",DoubleType,true),StructField("component_270",DoubleType,true),StructField("component_271",DoubleType,true),StructField("component_272",DoubleType,true),StructField("component_273",DoubleType,true),StructField("component_274",DoubleType,true),StructField("component_275",DoubleType,true),StructField("component_276",DoubleType,true),StructField("component_277",DoubleType,true),StructField("component_278",DoubleType,true),StructField("component_279",DoubleType,true),StructField("component_280",DoubleType,true),StructField("component_281",DoubleType,true),StructField("component_282",DoubleType,true),StructField("component_283",DoubleType,true),StructField("component_284",DoubleType,true),StructField("component_285",DoubleType,true),StructField("component_286",DoubleType,true),StructField("component_287",DoubleType,true),StructField("component_288",DoubleType,true),StructField("component_289",DoubleType,true),StructField("component_290",DoubleType,true),StructField("component_291",DoubleType,true),StructField("component_292",DoubleType,true),StructField("component_293",DoubleType,true),StructField("component_294",DoubleType,true),StructField("component_295",DoubleType,true),StructField("component_296",DoubleType,true),StructField("component_297",DoubleType,true),StructField("component_298",DoubleType,true),StructField("component_299",DoubleType,true),StructField("component_300",DoubleType,true),StructField("component_301",DoubleType,true),StructField("component_302",DoubleType,true),StructField("component_303",DoubleType,true),StructField("component_304",DoubleType,true),StructField("component_305",DoubleType,true),StructField("component_306",DoubleType,true),StructField("component_307",DoubleType,true),StructField("component_308",DoubleType,true),StructField("component_309",DoubleType,true),StructField("component_310",DoubleType,true),StructField("component_311",DoubleType,true),StructField("component_312",DoubleType,true),StructField("component_313",DoubleType,true),StructField("component_314",DoubleType,true),StructField("component_315",DoubleType,true),StructField("component_316",DoubleType,true),StructField("component_317",DoubleType,true),StructField("component_318",DoubleType,true),StructField("component_319",DoubleType,true),StructField("component_320",DoubleType,true),StructField("component_321",DoubleType,true),StructField("component_322",DoubleType,true),StructField("component_323",DoubleType,true),StructField("component_324",DoubleType,true),StructField("component_325",DoubleType,true),StructField("component_326",DoubleType,true),StructField("component_327",DoubleType,true),StructField("component_328",DoubleType,true),StructField("component_329",DoubleType,true),StructField("component_330",DoubleType,true),StructField("component_331",DoubleType,true),StructField("component_332",DoubleType,true),StructField("component_333",DoubleType,true),StructField("component_334",DoubleType,true),StructField("component_335",DoubleType,true),StructField("component_336",DoubleType,true),StructField("component_337",DoubleType,true),StructField("component_338",DoubleType,true),StructField("component_339",DoubleType,true),StructField("component_340",DoubleType,true),StructField("component_341",DoubleType,true),StructField("component_342",DoubleType,true),StructField("component_343",DoubleType,true),StructField("component_344",DoubleType,true),StructField("component_345",DoubleType,true),StructField("component_346",DoubleType,true),StructField("component_347",DoubleType,true),StructField("component_348",DoubleType,true),StructField("component_349",DoubleType,true),StructField("component_350",DoubleType,true),StructField("component_351",DoubleType,true),StructField("component_352",DoubleType,true),StructField("component_353",DoubleType,true),StructField("component_354",DoubleType,true),StructField("component_355",DoubleType,true),StructField("component_356",DoubleType,true),StructField("component_357",DoubleType,true),StructField("component_358",DoubleType,true),StructField("component_359",DoubleType,true),StructField("component_360",DoubleType,true),StructField("component_361",DoubleType,true),StructField("component_362",DoubleType,true),StructField("component_363",DoubleType,true),StructField("component_364",DoubleType,true),StructField("component_365",DoubleType,true),StructField("component_366",DoubleType,true),StructField("component_367",DoubleType,true),StructField("component_368",DoubleType,true),StructField("component_369",DoubleType,true),StructField("component_370",DoubleType,true),StructField("component_371",DoubleType,true),StructField("component_372",DoubleType,true),StructField("component_373",DoubleType,true),StructField("component_374",DoubleType,true),StructField("component_375",DoubleType,true),StructField("component_376",DoubleType,true),StructField("component_377",DoubleType,true),StructField("component_378",DoubleType,true),StructField("component_379",DoubleType,true),StructField("component_380",DoubleType,true),StructField("component_381",DoubleType,true),StructField("component_382",DoubleType,true),StructField("component_383",DoubleType,true),StructField("component_384",DoubleType,true),StructField("component_385",DoubleType,true),StructField("component_386",DoubleType,true),StructField("component_387",DoubleType,true),StructField("component_388",DoubleType,true),StructField("component_389",DoubleType,true),StructField("component_390",DoubleType,true),StructField("component_391",DoubleType,true),StructField("component_392",DoubleType,true),StructField("component_393",DoubleType,true),StructField("component_394",DoubleType,true),StructField("component_395",DoubleType,true),StructField("component_396",DoubleType,true),StructField("component_397",DoubleType,true),StructField("component_398",DoubleType,true),StructField("component_399",DoubleType,true),StructField("component_400",DoubleType,true),StructField("component_401",DoubleType,true),StructField("component_402",DoubleType,true),StructField("component_403",DoubleType,true),StructField("component_404",DoubleType,true),StructField("component_405",DoubleType,true),StructField("component_406",DoubleType,true),StructField("component_407",DoubleType,true),StructField("component_408",DoubleType,true),StructField("component_409",DoubleType,true),StructField("component_410",DoubleType,true),StructField("component_411",DoubleType,true),StructField("component_412",DoubleType,true),StructField("component_413",DoubleType,true),StructField("component_414",DoubleType,true),StructField("component_415",DoubleType,true),StructField("component_416",DoubleType,true),StructField("component_417",DoubleType,true),StructField("component_418",DoubleType,true),StructField("component_419",DoubleType,true),StructField(instanceName,StringType,true)))
# StructType(List(StructField("component_0",DoubleType,true),StructField("component_1",DoubleType,true),StructField("component_2",DoubleType,true),StructField("component_3",DoubleType,true),StructField("component_4",DoubleType,true),StructField("component_5",DoubleType,true),StructField("component_6",DoubleType,true),StructField("component_7",DoubleType,true),StructField("component_8",DoubleType,true),StructField("component_9",DoubleType,true),StructField("component_10",DoubleType,true),StructField("component_11",DoubleType,true),StructField("component_12",DoubleType,true),StructField("component_13",DoubleType,true),StructField("component_14",DoubleType,true),StructField("component_15",DoubleType,true),StructField("component_16",DoubleType,true),StructField("component_17",DoubleType,true),StructField("component_18",DoubleType,true),StructField("component_19",DoubleType,true),StructField("component_20",DoubleType,true),StructField("component_21",DoubleType,true),StructField("component_22",DoubleType,true),StructField("component_23",DoubleType,true),StructField("component_24",DoubleType,true),StructField("component_25",DoubleType,true),StructField("component_26",DoubleType,true),StructField("component_27",DoubleType,true),StructField("component_28",DoubleType,true),StructField("component_29",DoubleType,true),StructField("component_30",DoubleType,true),StructField("component_31",DoubleType,true),StructField("component_32",DoubleType,true),StructField("component_33",DoubleType,true),StructField("component_34",DoubleType,true),StructField("component_35",DoubleType,true),StructField("component_36",DoubleType,true),StructField("component_37",DoubleType,true),StructField("component_38",DoubleType,true),StructField("component_39",DoubleType,true),StructField("component_40",DoubleType,true),StructField("component_41",DoubleType,true),StructField("component_42",DoubleType,true),StructField("component_43",DoubleType,true),StructField("component_44",DoubleType,true),StructField("component_45",DoubleType,true),StructField("component_46",DoubleType,true),StructField("component_47",DoubleType,true),StructField("component_48",DoubleType,true),StructField("component_49",DoubleType,true),StructField("component_50",DoubleType,true),StructField("component_51",DoubleType,true),StructField("component_52",DoubleType,true),StructField("component_53",DoubleType,true),StructField("component_54",DoubleType,true),StructField("component_55",DoubleType,true),StructField("component_56",DoubleType,true),StructField("component_57",DoubleType,true),StructField("component_58",DoubleType,true),StructField("component_59",DoubleType,true),StructField(instanceName,StringType,true)))
# StructType(List(StructField("component_1",DoubleType,true),StructField("component_2",DoubleType,true),StructField("component_3",DoubleType,true),StructField("component_4",DoubleType,true),StructField("component_5",DoubleType,true),StructField("component_6",DoubleType,true),StructField("component_7",DoubleType,true),StructField("component_8",DoubleType,true),StructField("component_9",DoubleType,true),StructField("component_10",DoubleType,true),StructField("component_11",DoubleType,true),StructField("component_12",DoubleType,true),StructField("component_13",DoubleType,true),StructField("component_14",DoubleType,true),StructField("component_15",DoubleType,true),StructField("component_16",DoubleType,true),StructField("component_17",DoubleType,true),StructField("component_18",DoubleType,true),StructField("component_19",DoubleType,true),StructField("component_20",DoubleType,true),StructField("component_21",DoubleType,true),StructField("component_22",DoubleType,true),StructField("component_23",DoubleType,true),StructField("component_24",DoubleType,true),StructField("component_25",DoubleType,true),StructField("component_26",DoubleType,true),StructField("component_27",DoubleType,true),StructField("component_28",DoubleType,true),StructField("component_29",DoubleType,true),StructField("component_30",DoubleType,true),StructField("component_31",DoubleType,true),StructField("component_32",DoubleType,true),StructField("component_33",DoubleType,true),StructField("component_34",DoubleType,true),StructField("component_35",DoubleType,true),StructField("component_36",DoubleType,true),StructField("component_37",DoubleType,true),StructField("component_38",DoubleType,true),StructField("component_39",DoubleType,true),StructField("component_40",DoubleType,true),StructField("component_41",DoubleType,true),StructField("component_42",DoubleType,true),StructField("component_43",DoubleType,true),StructField("component_44",DoubleType,true),StructField("component_45",DoubleType,true),StructField("component_46",DoubleType,true),StructField("component_47",DoubleType,true),StructField("component_48",DoubleType,true),StructField("component_49",DoubleType,true),StructField("component_50",DoubleType,true),StructField("component_51",DoubleType,true),StructField("component_52",DoubleType,true),StructField("component_53",DoubleType,true),StructField("component_54",DoubleType,true),StructField("component_55",DoubleType,true),StructField("component_56",DoubleType,true),StructField("component_57",DoubleType,true),StructField("component_58",DoubleType,true),StructField("component_59",DoubleType,true),StructField("component_60",DoubleType,true),StructField("component_61",DoubleType,true),StructField("component_62",DoubleType,true),StructField("component_63",DoubleType,true),StructField("component_64",DoubleType,true),StructField("component_65",DoubleType,true),StructField("component_66",DoubleType,true),StructField("component_67",DoubleType,true),StructField("component_68",DoubleType,true),StructField("component_69",DoubleType,true),StructField("component_70",DoubleType,true),StructField("component_71",DoubleType,true),StructField("component_72",DoubleType,true),StructField("component_73",DoubleType,true),StructField("component_74",DoubleType,true),StructField("component_75",DoubleType,true),StructField("component_76",DoubleType,true),StructField("component_77",DoubleType,true),StructField("component_78",DoubleType,true),StructField("component_79",DoubleType,true),StructField("component_80",DoubleType,true),StructField("component_81",DoubleType,true),StructField("component_82",DoubleType,true),StructField("component_83",DoubleType,true),StructField("component_84",DoubleType,true),StructField("component_85",DoubleType,true),StructField("component_86",DoubleType,true),StructField("component_87",DoubleType,true),StructField("component_88",DoubleType,true),StructField("component_89",DoubleType,true),StructField("component_90",DoubleType,true),StructField("component_91",DoubleType,true),StructField("component_92",DoubleType,true),StructField("component_93",DoubleType,true),StructField("component_94",DoubleType,true),StructField("component_95",DoubleType,true),StructField("component_96",DoubleType,true),StructField("component_97",DoubleType,true),StructField("component_98",DoubleType,true),StructField("component_99",DoubleType,true),StructField("component_100",DoubleType,true),StructField("component_101",DoubleType,true),StructField("component_102",DoubleType,true),StructField("component_103",DoubleType,true),StructField("component_104",DoubleType,true),StructField("component_105",DoubleType,true),StructField("component_106",DoubleType,true),StructField("component_107",DoubleType,true),StructField("component_108",DoubleType,true),StructField("component_109",DoubleType,true),StructField("component_110",DoubleType,true),StructField("component_111",DoubleType,true),StructField("component_112",DoubleType,true),StructField("component_113",DoubleType,true),StructField("component_114",DoubleType,true),StructField("component_115",DoubleType,true),StructField("component_116",DoubleType,true),StructField("component_117",DoubleType,true),StructField("component_118",DoubleType,true),StructField("component_119",DoubleType,true),StructField("component_120",DoubleType,true),StructField("component_121",DoubleType,true),StructField("component_122",DoubleType,true),StructField("component_123",DoubleType,true),StructField("component_124",DoubleType,true),StructField("component_125",DoubleType,true),StructField("component_126",DoubleType,true),StructField("component_127",DoubleType,true),StructField("component_128",DoubleType,true),StructField("component_129",DoubleType,true),StructField("component_130",DoubleType,true),StructField("component_131",DoubleType,true),StructField("component_132",DoubleType,true),StructField("component_133",DoubleType,true),StructField("component_134",DoubleType,true),StructField("component_135",DoubleType,true),StructField("component_136",DoubleType,true),StructField("component_137",DoubleType,true),StructField("component_138",DoubleType,true),StructField("component_139",DoubleType,true),StructField("component_140",DoubleType,true),StructField("component_141",DoubleType,true),StructField("component_142",DoubleType,true),StructField("component_143",DoubleType,true),StructField("component_144",DoubleType,true),StructField("component_145",DoubleType,true),StructField("component_146",DoubleType,true),StructField("component_147",DoubleType,true),StructField("component_148",DoubleType,true),StructField("component_149",DoubleType,true),StructField("component_150",DoubleType,true),StructField("component_151",DoubleType,true),StructField("component_152",DoubleType,true),StructField("component_153",DoubleType,true),StructField("component_154",DoubleType,true),StructField("component_155",DoubleType,true),StructField("component_156",DoubleType,true),StructField("component_157",DoubleType,true),StructField("component_158",DoubleType,true),StructField("component_159",DoubleType,true),StructField("component_160",DoubleType,true),StructField("component_161",DoubleType,true),StructField("component_162",DoubleType,true),StructField("component_163",DoubleType,true),StructField("component_164",DoubleType,true),StructField("component_165",DoubleType,true),StructField("component_166",DoubleType,true),StructField("component_167",DoubleType,true),StructField("component_168",DoubleType,true),StructField("component_169",DoubleType,true),StructField("component_170",DoubleType,true),StructField("component_171",DoubleType,true),StructField("component_172",DoubleType,true),StructField("component_173",DoubleType,true),StructField("component_174",DoubleType,true),StructField("component_175",DoubleType,true),StructField("component_176",DoubleType,true),StructField("component_177",DoubleType,true),StructField("component_178",DoubleType,true),StructField("component_179",DoubleType,true),StructField("component_180",DoubleType,true),StructField("component_181",DoubleType,true),StructField("component_182",DoubleType,true),StructField("component_183",DoubleType,true),StructField("component_184",DoubleType,true),StructField("component_185",DoubleType,true),StructField("component_186",DoubleType,true),StructField("component_187",DoubleType,true),StructField("component_188",DoubleType,true),StructField("component_189",DoubleType,true),StructField("component_190",DoubleType,true),StructField("component_191",DoubleType,true),StructField("component_192",DoubleType,true),StructField("component_193",DoubleType,true),StructField("component_194",DoubleType,true),StructField("component_195",DoubleType,true),StructField("component_196",DoubleType,true),StructField("component_197",DoubleType,true),StructField("component_198",DoubleType,true),StructField("component_199",DoubleType,true),StructField("component_200",DoubleType,true),StructField("component_201",DoubleType,true),StructField("component_202",DoubleType,true),StructField("component_203",DoubleType,true),StructField("component_204",DoubleType,true),StructField("component_205",DoubleType,true),StructField("component_206",DoubleType,true),StructField("component_207",DoubleType,true),StructField("component_208",DoubleType,true),StructField("component_209",DoubleType,true),StructField("component_210",DoubleType,true),StructField("component_211",DoubleType,true),StructField("component_212",DoubleType,true),StructField("component_213",DoubleType,true),StructField("component_214",DoubleType,true),StructField("component_215",DoubleType,true),StructField("component_216",DoubleType,true),StructField("component_217",DoubleType,true),StructField("component_218",DoubleType,true),StructField("component_219",DoubleType,true),StructField("component_220",DoubleType,true),StructField("component_221",DoubleType,true),StructField("component_222",DoubleType,true),StructField("component_223",DoubleType,true),StructField("component_224",DoubleType,true),StructField("component_225",DoubleType,true),StructField("component_226",DoubleType,true),StructField("component_227",DoubleType,true),StructField("component_228",DoubleType,true),StructField("component_229",DoubleType,true),StructField("component_230",DoubleType,true),StructField("component_231",DoubleType,true),StructField("component_232",DoubleType,true),StructField("component_233",DoubleType,true),StructField("component_234",DoubleType,true),StructField("component_235",DoubleType,true),StructField("component_236",DoubleType,true),StructField("component_237",DoubleType,true),StructField("component_238",DoubleType,true),StructField("component_239",DoubleType,true),StructField("component_240",DoubleType,true),StructField("component_241",DoubleType,true),StructField("component_242",DoubleType,true),StructField("component_243",DoubleType,true),StructField("component_244",DoubleType,true),StructField("component_245",DoubleType,true),StructField("component_246",DoubleType,true),StructField("component_247",DoubleType,true),StructField("component_248",DoubleType,true),StructField("component_249",DoubleType,true),StructField("component_250",DoubleType,true),StructField("component_251",DoubleType,true),StructField("component_252",DoubleType,true),StructField("component_253",DoubleType,true),StructField("component_254",DoubleType,true),StructField("component_255",DoubleType,true),StructField("component_256",DoubleType,true),StructField("component_257",DoubleType,true),StructField("component_258",DoubleType,true),StructField("component_259",DoubleType,true),StructField("component_260",DoubleType,true),StructField("component_261",DoubleType,true),StructField("component_262",DoubleType,true),StructField("component_263",DoubleType,true),StructField("component_264",DoubleType,true),StructField("component_265",DoubleType,true),StructField("component_266",DoubleType,true),StructField("component_267",DoubleType,true),StructField("component_268",DoubleType,true),StructField("component_269",DoubleType,true),StructField("component_270",DoubleType,true),StructField("component_271",DoubleType,true),StructField("component_272",DoubleType,true),StructField("component_273",DoubleType,true),StructField("component_274",DoubleType,true),StructField("component_275",DoubleType,true),StructField("component_276",DoubleType,true),StructField("component_277",DoubleType,true),StructField("component_278",DoubleType,true),StructField("component_279",DoubleType,true),StructField("component_280",DoubleType,true),StructField("component_281",DoubleType,true),StructField("component_282",DoubleType,true),StructField("component_283",DoubleType,true),StructField("component_284",DoubleType,true),StructField("component_285",DoubleType,true),StructField("component_286",DoubleType,true),StructField("component_287",DoubleType,true),StructField("component_288",DoubleType,true),StructField("component_289",DoubleType,true),StructField("component_290",DoubleType,true),StructField("component_291",DoubleType,true),StructField("component_292",DoubleType,true),StructField("component_293",DoubleType,true),StructField("component_294",DoubleType,true),StructField("component_295",DoubleType,true),StructField("component_296",DoubleType,true),StructField("component_297",DoubleType,true),StructField("component_298",DoubleType,true),StructField("component_299",DoubleType,true),StructField("component_300",DoubleType,true),StructField("component_301",DoubleType,true),StructField("component_302",DoubleType,true),StructField("component_303",DoubleType,true),StructField("component_304",DoubleType,true),StructField("component_305",DoubleType,true),StructField("component_306",DoubleType,true),StructField("component_307",DoubleType,true),StructField("component_308",DoubleType,true),StructField("component_309",DoubleType,true),StructField("component_310",DoubleType,true),StructField("component_311",DoubleType,true),StructField("component_312",DoubleType,true),StructField("component_313",DoubleType,true),StructField("component_314",DoubleType,true),StructField("component_315",DoubleType,true),StructField("component_316",DoubleType,true),StructField("component_317",DoubleType,true),StructField("component_318",DoubleType,true),StructField("component_319",DoubleType,true),StructField("component_320",DoubleType,true),StructField("component_321",DoubleType,true),StructField("component_322",DoubleType,true),StructField("component_323",DoubleType,true),StructField("component_324",DoubleType,true),StructField("component_325",DoubleType,true),StructField("component_326",DoubleType,true),StructField("component_327",DoubleType,true),StructField("component_328",DoubleType,true),StructField("component_329",DoubleType,true),StructField("component_330",DoubleType,true),StructField("component_331",DoubleType,true),StructField("component_332",DoubleType,true),StructField("component_333",DoubleType,true),StructField("component_334",DoubleType,true),StructField("component_335",DoubleType,true),StructField("component_336",DoubleType,true),StructField("component_337",DoubleType,true),StructField("component_338",DoubleType,true),StructField("component_339",DoubleType,true),StructField("component_340",DoubleType,true),StructField("component_341",DoubleType,true),StructField("component_342",DoubleType,true),StructField("component_343",DoubleType,true),StructField("component_344",DoubleType,true),StructField("component_345",DoubleType,true),StructField("component_346",DoubleType,true),StructField("component_347",DoubleType,true),StructField("component_348",DoubleType,true),StructField("component_349",DoubleType,true),StructField("component_350",DoubleType,true),StructField("component_351",DoubleType,true),StructField("component_352",DoubleType,true),StructField("component_353",DoubleType,true),StructField("component_354",DoubleType,true),StructField("component_355",DoubleType,true),StructField("component_356",DoubleType,true),StructField("component_357",DoubleType,true),StructField("component_358",DoubleType,true),StructField("component_359",DoubleType,true),StructField("component_360",DoubleType,true),StructField("component_361",DoubleType,true),StructField("component_362",DoubleType,true),StructField("component_363",DoubleType,true),StructField("component_364",DoubleType,true),StructField("component_365",DoubleType,true),StructField("component_366",DoubleType,true),StructField("component_367",DoubleType,true),StructField("component_368",DoubleType,true),StructField("component_369",DoubleType,true),StructField("component_370",DoubleType,true),StructField("component_371",DoubleType,true),StructField("component_372",DoubleType,true),StructField("component_373",DoubleType,true),StructField("component_374",DoubleType,true),StructField("component_375",DoubleType,true),StructField("component_376",DoubleType,true),StructField("component_377",DoubleType,true),StructField("component_378",DoubleType,true),StructField("component_379",DoubleType,true),StructField("component_380",DoubleType,true),StructField("component_381",DoubleType,true),StructField("component_382",DoubleType,true),StructField("component_383",DoubleType,true),StructField("component_384",DoubleType,true),StructField("component_385",DoubleType,true),StructField("component_386",DoubleType,true),StructField("component_387",DoubleType,true),StructField("component_388",DoubleType,true),StructField("component_389",DoubleType,true),StructField("component_390",DoubleType,true),StructField("component_391",DoubleType,true),StructField("component_392",DoubleType,true),StructField("component_393",DoubleType,true),StructField("component_394",DoubleType,true),StructField("component_395",DoubleType,true),StructField("component_396",DoubleType,true),StructField("component_397",DoubleType,true),StructField("component_398",DoubleType,true),StructField("component_399",DoubleType,true),StructField("component_400",DoubleType,true),StructField("component_401",DoubleType,true),StructField("component_402",DoubleType,true),StructField("component_403",DoubleType,true),StructField("component_404",DoubleType,true),StructField("component_405",DoubleType,true),StructField("component_406",DoubleType,true),StructField("component_407",DoubleType,true),StructField("component_408",DoubleType,true),StructField("component_409",DoubleType,true),StructField("component_410",DoubleType,true),StructField("component_411",DoubleType,true),StructField("component_412",DoubleType,true),StructField("component_413",DoubleType,true),StructField("component_414",DoubleType,true),StructField("component_415",DoubleType,true),StructField("component_416",DoubleType,true),StructField("component_417",DoubleType,true),StructField("component_418",DoubleType,true),StructField("component_419",DoubleType,true),StructField("component_420",DoubleType,true),StructField("component_421",DoubleType,true),StructField("component_422",DoubleType,true),StructField("component_423",DoubleType,true),StructField("component_424",DoubleType,true),StructField("component_425",DoubleType,true),StructField("component_426",DoubleType,true),StructField("component_427",DoubleType,true),StructField("component_428",DoubleType,true),StructField("component_429",DoubleType,true),StructField("component_430",DoubleType,true),StructField("component_431",DoubleType,true),StructField("component_432",DoubleType,true),StructField("component_433",DoubleType,true),StructField("component_434",DoubleType,true),StructField("component_435",DoubleType,true),StructField("component_436",DoubleType,true),StructField("component_437",DoubleType,true),StructField("component_438",DoubleType,true),StructField("component_439",DoubleType,true),StructField("component_440",DoubleType,true),StructField("component_441",DoubleType,true),StructField("component_442",DoubleType,true),StructField("component_443",DoubleType,true),StructField("component_444",DoubleType,true),StructField("component_445",DoubleType,true),StructField("component_446",DoubleType,true),StructField("component_447",DoubleType,true),StructField("component_448",DoubleType,true),StructField("component_449",DoubleType,true),StructField("component_450",DoubleType,true),StructField("component_451",DoubleType,true),StructField("component_452",DoubleType,true),StructField("component_453",DoubleType,true),StructField("component_454",DoubleType,true),StructField("component_455",DoubleType,true),StructField("component_456",DoubleType,true),StructField("component_457",DoubleType,true),StructField("component_458",DoubleType,true),StructField("component_459",DoubleType,true),StructField("component_460",DoubleType,true),StructField("component_461",DoubleType,true),StructField("component_462",DoubleType,true),StructField("component_463",DoubleType,true),StructField("component_464",DoubleType,true),StructField("component_465",DoubleType,true),StructField("component_466",DoubleType,true),StructField("component_467",DoubleType,true),StructField("component_468",DoubleType,true),StructField("component_469",DoubleType,true),StructField("component_470",DoubleType,true),StructField("component_471",DoubleType,true),StructField("component_472",DoubleType,true),StructField("component_473",DoubleType,true),StructField("component_474",DoubleType,true),StructField("component_475",DoubleType,true),StructField("component_476",DoubleType,true),StructField("component_477",DoubleType,true),StructField("component_478",DoubleType,true),StructField("component_479",DoubleType,true),StructField("component_480",DoubleType,true),StructField("component_481",DoubleType,true),StructField("component_482",DoubleType,true),StructField("component_483",DoubleType,true),StructField("component_484",DoubleType,true),StructField("component_485",DoubleType,true),StructField("component_486",DoubleType,true),StructField("component_487",DoubleType,true),StructField("component_488",DoubleType,true),StructField("component_489",DoubleType,true),StructField("component_490",DoubleType,true),StructField("component_491",DoubleType,true),StructField("component_492",DoubleType,true),StructField("component_493",DoubleType,true),StructField("component_494",DoubleType,true),StructField("component_495",DoubleType,true),StructField("component_496",DoubleType,true),StructField("component_497",DoubleType,true),StructField("component_498",DoubleType,true),StructField("component_499",DoubleType,true),StructField("component_500",DoubleType,true),StructField("component_501",DoubleType,true),StructField("component_502",DoubleType,true),StructField("component_503",DoubleType,true),StructField("component_504",DoubleType,true),StructField("component_505",DoubleType,true),StructField("component_506",DoubleType,true),StructField("component_507",DoubleType,true),StructField("component_508",DoubleType,true),StructField("component_509",DoubleType,true),StructField("component_510",DoubleType,true),StructField("component_511",DoubleType,true),StructField("component_512",DoubleType,true),StructField("component_513",DoubleType,true),StructField("component_514",DoubleType,true),StructField("component_515",DoubleType,true),StructField("component_516",DoubleType,true),StructField("component_517",DoubleType,true),StructField("component_518",DoubleType,true),StructField("component_519",DoubleType,true),StructField("component_520",DoubleType,true),StructField("component_521",DoubleType,true),StructField("component_522",DoubleType,true),StructField("component_523",DoubleType,true),StructField("component_524",DoubleType,true),StructField("component_525",DoubleType,true),StructField("component_526",DoubleType,true),StructField("component_527",DoubleType,true),StructField("component_528",DoubleType,true),StructField("component_529",DoubleType,true),StructField("component_530",DoubleType,true),StructField("component_531",DoubleType,true),StructField("component_532",DoubleType,true),StructField("component_533",DoubleType,true),StructField("component_534",DoubleType,true),StructField("component_535",DoubleType,true),StructField("component_536",DoubleType,true),StructField("component_537",DoubleType,true),StructField("component_538",DoubleType,true),StructField("component_539",DoubleType,true),StructField("component_540",DoubleType,true),StructField("component_541",DoubleType,true),StructField("component_542",DoubleType,true),StructField("component_543",DoubleType,true),StructField("component_544",DoubleType,true),StructField("component_545",DoubleType,true),StructField("component_546",DoubleType,true),StructField("component_547",DoubleType,true),StructField("component_548",DoubleType,true),StructField("component_549",DoubleType,true),StructField("component_550",DoubleType,true),StructField("component_551",DoubleType,true),StructField("component_552",DoubleType,true),StructField("component_553",DoubleType,true),StructField("component_554",DoubleType,true),StructField("component_555",DoubleType,true),StructField("component_556",DoubleType,true),StructField("component_557",DoubleType,true),StructField("component_558",DoubleType,true),StructField("component_559",DoubleType,true),StructField("component_560",DoubleType,true),StructField("component_561",DoubleType,true),StructField("component_562",DoubleType,true),StructField("component_563",DoubleType,true),StructField("component_564",DoubleType,true),StructField("component_565",DoubleType,true),StructField("component_566",DoubleType,true),StructField("component_567",DoubleType,true),StructField("component_568",DoubleType,true),StructField("component_569",DoubleType,true),StructField("component_570",DoubleType,true),StructField("component_571",DoubleType,true),StructField("component_572",DoubleType,true),StructField("component_573",DoubleType,true),StructField("component_574",DoubleType,true),StructField("component_575",DoubleType,true),StructField("component_576",DoubleType,true),StructField("component_577",DoubleType,true),StructField("component_578",DoubleType,true),StructField("component_579",DoubleType,true),StructField("component_580",DoubleType,true),StructField("component_581",DoubleType,true),StructField("component_582",DoubleType,true),StructField("component_583",DoubleType,true),StructField("component_584",DoubleType,true),StructField("component_585",DoubleType,true),StructField("component_586",DoubleType,true),StructField("component_587",DoubleType,true),StructField("component_588",DoubleType,true),StructField("component_589",DoubleType,true),StructField("component_590",DoubleType,true),StructField("component_591",DoubleType,true),StructField("component_592",DoubleType,true),StructField("component_593",DoubleType,true),StructField("component_594",DoubleType,true),StructField("component_595",DoubleType,true),StructField("component_596",DoubleType,true),StructField("component_597",DoubleType,true),StructField("component_598",DoubleType,true),StructField("component_599",DoubleType,true),StructField("component_600",DoubleType,true),StructField("component_601",DoubleType,true),StructField("component_602",DoubleType,true),StructField("component_603",DoubleType,true),StructField("component_604",DoubleType,true),StructField("component_605",DoubleType,true),StructField("component_606",DoubleType,true),StructField("component_607",DoubleType,true),StructField("component_608",DoubleType,true),StructField("component_609",DoubleType,true),StructField("component_610",DoubleType,true),StructField("component_611",DoubleType,true),StructField("component_612",DoubleType,true),StructField("component_613",DoubleType,true),StructField("component_614",DoubleType,true),StructField("component_615",DoubleType,true),StructField("component_616",DoubleType,true),StructField("component_617",DoubleType,true),StructField("component_618",DoubleType,true),StructField("component_619",DoubleType,true),StructField("component_620",DoubleType,true),StructField("component_621",DoubleType,true),StructField("component_622",DoubleType,true),StructField("component_623",DoubleType,true),StructField("component_624",DoubleType,true),StructField("component_625",DoubleType,true),StructField("component_626",DoubleType,true),StructField("component_627",DoubleType,true),StructField("component_628",DoubleType,true),StructField("component_629",DoubleType,true),StructField("component_630",DoubleType,true),StructField("component_631",DoubleType,true),StructField("component_632",DoubleType,true),StructField("component_633",DoubleType,true),StructField("component_634",DoubleType,true),StructField("component_635",DoubleType,true),StructField("component_636",DoubleType,true),StructField("component_637",DoubleType,true),StructField("component_638",DoubleType,true),StructField("component_639",DoubleType,true),StructField("component_640",DoubleType,true),StructField("component_641",DoubleType,true),StructField("component_642",DoubleType,true),StructField("component_643",DoubleType,true),StructField("component_644",DoubleType,true),StructField("component_645",DoubleType,true),StructField("component_646",DoubleType,true),StructField("component_647",DoubleType,true),StructField("component_648",DoubleType,true),StructField("component_649",DoubleType,true),StructField("component_650",DoubleType,true),StructField("component_651",DoubleType,true),StructField("component_652",DoubleType,true),StructField("component_653",DoubleType,true),StructField("component_654",DoubleType,true),StructField("component_655",DoubleType,true),StructField("component_656",DoubleType,true),StructField("component_657",DoubleType,true),StructField("component_658",DoubleType,true),StructField("component_659",DoubleType,true),StructField("component_660",DoubleType,true),StructField("component_661",DoubleType,true),StructField("component_662",DoubleType,true),StructField("component_663",DoubleType,true),StructField("component_664",DoubleType,true),StructField("component_665",DoubleType,true),StructField("component_666",DoubleType,true),StructField("component_667",DoubleType,true),StructField("component_668",DoubleType,true),StructField("component_669",DoubleType,true),StructField("component_670",DoubleType,true),StructField("component_671",DoubleType,true),StructField("component_672",DoubleType,true),StructField("component_673",DoubleType,true),StructField("component_674",DoubleType,true),StructField("component_675",DoubleType,true),StructField("component_676",DoubleType,true),StructField("component_677",DoubleType,true),StructField("component_678",DoubleType,true),StructField("component_679",DoubleType,true),StructField("component_680",DoubleType,true),StructField("component_681",DoubleType,true),StructField("component_682",DoubleType,true),StructField("component_683",DoubleType,true),StructField("component_684",DoubleType,true),StructField("component_685",DoubleType,true),StructField("component_686",DoubleType,true),StructField("component_687",DoubleType,true),StructField("component_688",DoubleType,true),StructField("component_689",DoubleType,true),StructField("component_690",DoubleType,true),StructField("component_691",DoubleType,true),StructField("component_692",DoubleType,true),StructField("component_693",DoubleType,true),StructField("component_694",DoubleType,true),StructField("component_695",DoubleType,true),StructField("component_696",DoubleType,true),StructField("component_697",DoubleType,true),StructField("component_698",DoubleType,true),StructField("component_699",DoubleType,true),StructField("component_700",DoubleType,true),StructField("component_701",DoubleType,true),StructField("component_702",DoubleType,true),StructField("component_703",DoubleType,true),StructField("component_704",DoubleType,true),StructField("component_705",DoubleType,true),StructField("component_706",DoubleType,true),StructField("component_707",DoubleType,true),StructField("component_708",DoubleType,true),StructField("component_709",DoubleType,true),StructField("component_710",DoubleType,true),StructField("component_711",DoubleType,true),StructField("component_712",DoubleType,true),StructField("component_713",DoubleType,true),StructField("component_714",DoubleType,true),StructField("component_715",DoubleType,true),StructField("component_716",DoubleType,true),StructField("component_717",DoubleType,true),StructField("component_718",DoubleType,true),StructField("component_719",DoubleType,true),StructField("component_720",DoubleType,true),StructField("component_721",DoubleType,true),StructField("component_722",DoubleType,true),StructField("component_723",DoubleType,true),StructField("component_724",DoubleType,true),StructField("component_725",DoubleType,true),StructField("component_726",DoubleType,true),StructField("component_727",DoubleType,true),StructField("component_728",DoubleType,true),StructField("component_729",DoubleType,true),StructField("component_730",DoubleType,true),StructField("component_731",DoubleType,true),StructField("component_732",DoubleType,true),StructField("component_733",DoubleType,true),StructField("component_734",DoubleType,true),StructField("component_735",DoubleType,true),StructField("component_736",DoubleType,true),StructField("component_737",DoubleType,true),StructField("component_738",DoubleType,true),StructField("component_739",DoubleType,true),StructField("component_740",DoubleType,true),StructField("component_741",DoubleType,true),StructField("component_742",DoubleType,true),StructField("component_743",DoubleType,true),StructField("component_744",DoubleType,true),StructField("component_745",DoubleType,true),StructField("component_746",DoubleType,true),StructField("component_747",DoubleType,true),StructField("component_748",DoubleType,true),StructField("component_749",DoubleType,true),StructField("component_750",DoubleType,true),StructField("component_751",DoubleType,true),StructField("component_752",DoubleType,true),StructField("component_753",DoubleType,true),StructField("component_754",DoubleType,true),StructField("component_755",DoubleType,true),StructField("component_756",DoubleType,true),StructField("component_757",DoubleType,true),StructField("component_758",DoubleType,true),StructField("component_759",DoubleType,true),StructField("component_760",DoubleType,true),StructField("component_761",DoubleType,true),StructField("component_762",DoubleType,true),StructField("component_763",DoubleType,true),StructField("component_764",DoubleType,true),StructField("component_765",DoubleType,true),StructField("component_766",DoubleType,true),StructField("component_767",DoubleType,true),StructField("component_768",DoubleType,true),StructField("component_769",DoubleType,true),StructField("component_770",DoubleType,true),StructField("component_771",DoubleType,true),StructField("component_772",DoubleType,true),StructField("component_773",DoubleType,true),StructField("component_774",DoubleType,true),StructField("component_775",DoubleType,true),StructField("component_776",DoubleType,true),StructField("component_777",DoubleType,true),StructField("component_778",DoubleType,true),StructField("component_779",DoubleType,true),StructField("component_780",DoubleType,true),StructField("component_781",DoubleType,true),StructField("component_782",DoubleType,true),StructField("component_783",DoubleType,true),StructField("component_784",DoubleType,true),StructField("component_785",DoubleType,true),StructField("component_786",DoubleType,true),StructField("component_787",DoubleType,true),StructField("component_788",DoubleType,true),StructField("component_789",DoubleType,true),StructField("component_790",DoubleType,true),StructField("component_791",DoubleType,true),StructField("component_792",DoubleType,true),StructField("component_793",DoubleType,true),StructField("component_794",DoubleType,true),StructField("component_795",DoubleType,true),StructField("component_796",DoubleType,true),StructField("component_797",DoubleType,true),StructField("component_798",DoubleType,true),StructField("component_799",DoubleType,true),StructField("component_800",DoubleType,true),StructField("component_801",DoubleType,true),StructField("component_802",DoubleType,true),StructField("component_803",DoubleType,true),StructField("component_804",DoubleType,true),StructField("component_805",DoubleType,true),StructField("component_806",DoubleType,true),StructField("component_807",DoubleType,true),StructField("component_808",DoubleType,true),StructField("component_809",DoubleType,true),StructField("component_810",DoubleType,true),StructField("component_811",DoubleType,true),StructField("component_812",DoubleType,true),StructField("component_813",DoubleType,true),StructField("component_814",DoubleType,true),StructField("component_815",DoubleType,true),StructField("component_816",DoubleType,true),StructField("component_817",DoubleType,true),StructField("component_818",DoubleType,true),StructField("component_819",DoubleType,true),StructField("component_820",DoubleType,true),StructField("component_821",DoubleType,true),StructField("component_822",DoubleType,true),StructField("component_823",DoubleType,true),StructField("component_824",DoubleType,true),StructField("component_825",DoubleType,true),StructField("component_826",DoubleType,true),StructField("component_827",DoubleType,true),StructField("component_828",DoubleType,true),StructField("component_829",DoubleType,true),StructField("component_830",DoubleType,true),StructField("component_831",DoubleType,true),StructField("component_832",DoubleType,true),StructField("component_833",DoubleType,true),StructField("component_834",DoubleType,true),StructField("component_835",DoubleType,true),StructField("component_836",DoubleType,true),StructField("component_837",DoubleType,true),StructField("component_838",DoubleType,true),StructField("component_839",DoubleType,true),StructField("component_840",DoubleType,true),StructField("component_841",DoubleType,true),StructField("component_842",DoubleType,true),StructField("component_843",DoubleType,true),StructField("component_844",DoubleType,true),StructField("component_845",DoubleType,true),StructField("component_846",DoubleType,true),StructField("component_847",DoubleType,true),StructField("component_848",DoubleType,true),StructField("component_849",DoubleType,true),StructField("component_850",DoubleType,true),StructField("component_851",DoubleType,true),StructField("component_852",DoubleType,true),StructField("component_853",DoubleType,true),StructField("component_854",DoubleType,true),StructField("component_855",DoubleType,true),StructField("component_856",DoubleType,true),StructField("component_857",DoubleType,true),StructField("component_858",DoubleType,true),StructField("component_859",DoubleType,true),StructField("component_860",DoubleType,true),StructField("component_861",DoubleType,true),StructField("component_862",DoubleType,true),StructField("component_863",DoubleType,true),StructField("component_864",DoubleType,true),StructField("component_865",DoubleType,true),StructField("component_866",DoubleType,true),StructField("component_867",DoubleType,true),StructField("component_868",DoubleType,true),StructField("component_869",DoubleType,true),StructField("component_870",DoubleType,true),StructField("component_871",DoubleType,true),StructField("component_872",DoubleType,true),StructField("component_873",DoubleType,true),StructField("component_874",DoubleType,true),StructField("component_875",DoubleType,true),StructField("component_876",DoubleType,true),StructField("component_877",DoubleType,true),StructField("component_878",DoubleType,true),StructField("component_879",DoubleType,true),StructField("component_880",DoubleType,true),StructField("component_881",DoubleType,true),StructField("component_882",DoubleType,true),StructField("component_883",DoubleType,true),StructField("component_884",DoubleType,true),StructField("component_885",DoubleType,true),StructField("component_886",DoubleType,true),StructField("component_887",DoubleType,true),StructField("component_888",DoubleType,true),StructField("component_889",DoubleType,true),StructField("component_890",DoubleType,true),StructField("component_891",DoubleType,true),StructField("component_892",DoubleType,true),StructField("component_893",DoubleType,true),StructField("component_894",DoubleType,true),StructField("component_895",DoubleType,true),StructField("component_896",DoubleType,true),StructField("component_897",DoubleType,true),StructField("component_898",DoubleType,true),StructField("component_899",DoubleType,true),StructField("component_900",DoubleType,true),StructField("component_901",DoubleType,true),StructField("component_902",DoubleType,true),StructField("component_903",DoubleType,true),StructField("component_904",DoubleType,true),StructField("component_905",DoubleType,true),StructField("component_906",DoubleType,true),StructField("component_907",DoubleType,true),StructField("component_908",DoubleType,true),StructField("component_909",DoubleType,true),StructField("component_910",DoubleType,true),StructField("component_911",DoubleType,true),StructField("component_912",DoubleType,true),StructField("component_913",DoubleType,true),StructField("component_914",DoubleType,true),StructField("component_915",DoubleType,true),StructField("component_916",DoubleType,true),StructField("component_917",DoubleType,true),StructField("component_918",DoubleType,true),StructField("component_919",DoubleType,true),StructField("component_920",DoubleType,true),StructField("component_921",DoubleType,true),StructField("component_922",DoubleType,true),StructField("component_923",DoubleType,true),StructField("component_924",DoubleType,true),StructField("component_925",DoubleType,true),StructField("component_926",DoubleType,true),StructField("component_927",DoubleType,true),StructField("component_928",DoubleType,true),StructField("component_929",DoubleType,true),StructField("component_930",DoubleType,true),StructField("component_931",DoubleType,true),StructField("component_932",DoubleType,true),StructField("component_933",DoubleType,true),StructField("component_934",DoubleType,true),StructField("component_935",DoubleType,true),StructField("component_936",DoubleType,true),StructField("component_937",DoubleType,true),StructField("component_938",DoubleType,true),StructField("component_939",DoubleType,true),StructField("component_940",DoubleType,true),StructField("component_941",DoubleType,true),StructField("component_942",DoubleType,true),StructField("component_943",DoubleType,true),StructField("component_944",DoubleType,true),StructField("component_945",DoubleType,true),StructField("component_946",DoubleType,true),StructField("component_947",DoubleType,true),StructField("component_948",DoubleType,true),StructField("component_949",DoubleType,true),StructField("component_950",DoubleType,true),StructField("component_951",DoubleType,true),StructField("component_952",DoubleType,true),StructField("component_953",DoubleType,true),StructField("component_954",DoubleType,true),StructField("component_955",DoubleType,true),StructField("component_956",DoubleType,true),StructField("component_957",DoubleType,true),StructField("component_958",DoubleType,true),StructField("component_959",DoubleType,true),StructField("component_960",DoubleType,true),StructField("component_961",DoubleType,true),StructField("component_962",DoubleType,true),StructField("component_963",DoubleType,true),StructField("component_964",DoubleType,true),StructField("component_965",DoubleType,true),StructField("component_966",DoubleType,true),StructField("component_967",DoubleType,true),StructField("component_968",DoubleType,true),StructField("component_969",DoubleType,true),StructField("component_970",DoubleType,true),StructField("component_971",DoubleType,true),StructField("component_972",DoubleType,true),StructField("component_973",DoubleType,true),StructField("component_974",DoubleType,true),StructField("component_975",DoubleType,true),StructField("component_976",DoubleType,true),StructField("component_977",DoubleType,true),StructField("component_978",DoubleType,true),StructField("component_979",DoubleType,true),StructField("component_980",DoubleType,true),StructField("component_981",DoubleType,true),StructField("component_982",DoubleType,true),StructField("component_983",DoubleType,true),StructField("component_984",DoubleType,true),StructField("component_985",DoubleType,true),StructField("component_986",DoubleType,true),StructField("component_987",DoubleType,true),StructField("component_988",DoubleType,true),StructField("component_989",DoubleType,true),StructField("component_990",DoubleType,true),StructField("component_991",DoubleType,true),StructField("component_992",DoubleType,true),StructField("component_993",DoubleType,true),StructField("component_994",DoubleType,true),StructField("component_995",DoubleType,true),StructField("component_996",DoubleType,true),StructField("component_997",DoubleType,true),StructField("component_998",DoubleType,true),StructField("component_999",DoubleType,true),StructField("component_1000",DoubleType,true),StructField("component_1001",DoubleType,true),StructField("component_1002",DoubleType,true),StructField("component_1003",DoubleType,true),StructField("component_1004",DoubleType,true),StructField("component_1005",DoubleType,true),StructField("component_1006",DoubleType,true),StructField("component_1007",DoubleType,true),StructField("component_1008",DoubleType,true),StructField("component_1009",DoubleType,true),StructField("component_1010",DoubleType,true),StructField("component_1011",DoubleType,true),StructField("component_1012",DoubleType,true),StructField("component_1013",DoubleType,true),StructField("component_1014",DoubleType,true),StructField("component_1015",DoubleType,true),StructField("component_1016",DoubleType,true),StructField("component_1017",DoubleType,true),StructField("component_1018",DoubleType,true),StructField("component_1019",DoubleType,true),StructField("component_1020",DoubleType,true),StructField("component_1021",DoubleType,true),StructField("component_1022",DoubleType,true),StructField("component_1023",DoubleType,true),StructField("component_1024",DoubleType,true),StructField("component_1025",DoubleType,true),StructField("component_1026",DoubleType,true),StructField("component_1027",DoubleType,true),StructField("component_1028",DoubleType,true),StructField("component_1029",DoubleType,true),StructField("component_1030",DoubleType,true),StructField("component_1031",DoubleType,true),StructField("component_1032",DoubleType,true),StructField("component_1033",DoubleType,true),StructField("component_1034",DoubleType,true),StructField("component_1035",DoubleType,true),StructField("component_1036",DoubleType,true),StructField("component_1037",DoubleType,true),StructField("component_1038",DoubleType,true),StructField("component_1039",DoubleType,true),StructField("component_1040",DoubleType,true),StructField("component_1041",DoubleType,true),StructField("component_1042",DoubleType,true),StructField("component_1043",DoubleType,true),StructField("component_1044",DoubleType,true),StructField("component_1045",DoubleType,true),StructField("component_1046",DoubleType,true),StructField("component_1047",DoubleType,true),StructField("component_1048",DoubleType,true),StructField("component_1049",DoubleType,true),StructField("component_1050",DoubleType,true),StructField("component_1051",DoubleType,true),StructField("component_1052",DoubleType,true),StructField("component_1053",DoubleType,true),StructField("component_1054",DoubleType,true),StructField("component_1055",DoubleType,true),StructField("component_1056",DoubleType,true),StructField("component_1057",DoubleType,true),StructField("component_1058",DoubleType,true),StructField("component_1059",DoubleType,true),StructField("component_1060",DoubleType,true),StructField("component_1061",DoubleType,true),StructField("component_1062",DoubleType,true),StructField("component_1063",DoubleType,true),StructField("component_1064",DoubleType,true),StructField("component_1065",DoubleType,true),StructField("component_1066",DoubleType,true),StructField("component_1067",DoubleType,true),StructField("component_1068",DoubleType,true),StructField("component_1069",DoubleType,true),StructField("component_1070",DoubleType,true),StructField("component_1071",DoubleType,true),StructField("component_1072",DoubleType,true),StructField("component_1073",DoubleType,true),StructField("component_1074",DoubleType,true),StructField("component_1075",DoubleType,true),StructField("component_1076",DoubleType,true),StructField("component_1077",DoubleType,true),StructField("component_1078",DoubleType,true),StructField("component_1079",DoubleType,true),StructField("component_1080",DoubleType,true),StructField("component_1081",DoubleType,true),StructField("component_1082",DoubleType,true),StructField("component_1083",DoubleType,true),StructField("component_1084",DoubleType,true),StructField("component_1085",DoubleType,true),StructField("component_1086",DoubleType,true),StructField("component_1087",DoubleType,true),StructField("component_1088",DoubleType,true),StructField("component_1089",DoubleType,true),StructField("component_1090",DoubleType,true),StructField("component_1091",DoubleType,true),StructField("component_1092",DoubleType,true),StructField("component_1093",DoubleType,true),StructField("component_1094",DoubleType,true),StructField("component_1095",DoubleType,true),StructField("component_1096",DoubleType,true),StructField("component_1097",DoubleType,true),StructField("component_1098",DoubleType,true),StructField("component_1099",DoubleType,true),StructField("component_1100",DoubleType,true),StructField("component_1101",DoubleType,true),StructField("component_1102",DoubleType,true),StructField("component_1103",DoubleType,true),StructField("component_1104",DoubleType,true),StructField("component_1105",DoubleType,true),StructField("component_1106",DoubleType,true),StructField("component_1107",DoubleType,true),StructField("component_1108",DoubleType,true),StructField("component_1109",DoubleType,true),StructField("component_1110",DoubleType,true),StructField("component_1111",DoubleType,true),StructField("component_1112",DoubleType,true),StructField("component_1113",DoubleType,true),StructField("component_1114",DoubleType,true),StructField("component_1115",DoubleType,true),StructField("component_1116",DoubleType,true),StructField("component_1117",DoubleType,true),StructField("component_1118",DoubleType,true),StructField("component_1119",DoubleType,true),StructField("component_1120",DoubleType,true),StructField("component_1121",DoubleType,true),StructField("component_1122",DoubleType,true),StructField("component_1123",DoubleType,true),StructField("component_1124",DoubleType,true),StructField("component_1125",DoubleType,true),StructField("component_1126",DoubleType,true),StructField("component_1127",DoubleType,true),StructField("component_1128",DoubleType,true),StructField("component_1129",DoubleType,true),StructField("component_1130",DoubleType,true),StructField("component_1131",DoubleType,true),StructField("component_1132",DoubleType,true),StructField("component_1133",DoubleType,true),StructField("component_1134",DoubleType,true),StructField("component_1135",DoubleType,true),StructField("component_1136",DoubleType,true),StructField("component_1137",DoubleType,true),StructField("component_1138",DoubleType,true),StructField("component_1139",DoubleType,true),StructField("component_1140",DoubleType,true),StructField("component_1141",DoubleType,true),StructField("component_1142",DoubleType,true),StructField("component_1143",DoubleType,true),StructField("component_1144",DoubleType,true),StructField("component_1145",DoubleType,true),StructField("component_1146",DoubleType,true),StructField("component_1147",DoubleType,true),StructField("component_1148",DoubleType,true),StructField("component_1149",DoubleType,true),StructField("component_1150",DoubleType,true),StructField("component_1151",DoubleType,true),StructField("component_1152",DoubleType,true),StructField("component_1153",DoubleType,true),StructField("component_1154",DoubleType,true),StructField("component_1155",DoubleType,true),StructField("component_1156",DoubleType,true),StructField("component_1157",DoubleType,true),StructField("component_1158",DoubleType,true),StructField("component_1159",DoubleType,true),StructField("component_1160",DoubleType,true),StructField("component_1161",DoubleType,true),StructField("component_1162",DoubleType,true),StructField("component_1163",DoubleType,true),StructField("component_1164",DoubleType,true),StructField("component_1165",DoubleType,true),StructField("component_1166",DoubleType,true),StructField("component_1167",DoubleType,true),StructField("component_1168",DoubleType,true),StructField("component_1169",DoubleType,true),StructField("component_1170",DoubleType,true),StructField("component_1171",DoubleType,true),StructField("component_1172",DoubleType,true),StructField("component_1173",DoubleType,true),StructField("component_1174",DoubleType,true),StructField("component_1175",DoubleType,true),StructField("component_1176",DoubleType,true),StructField("component_1177",DoubleType,true),StructField("component_1178",DoubleType,true),StructField("component_1179",DoubleType,true),StructField("component_1180",DoubleType,true),StructField("component_1181",DoubleType,true),StructField("component_1182",DoubleType,true),StructField("component_1183",DoubleType,true),StructField("component_1184",DoubleType,true),StructField("component_1185",DoubleType,true),StructField("component_1186",DoubleType,true),StructField("component_1187",DoubleType,true),StructField("component_1188",DoubleType,true),StructField("component_1189",DoubleType,true),StructField("component_1190",DoubleType,true),StructField("component_1191",DoubleType,true),StructField("component_1192",DoubleType,true),StructField("component_1193",DoubleType,true),StructField("component_1194",DoubleType,true),StructField("component_1195",DoubleType,true),StructField("component_1196",DoubleType,true),StructField("component_1197",DoubleType,true),StructField("component_1198",DoubleType,true),StructField("component_1199",DoubleType,true),StructField("component_1200",DoubleType,true),StructField("component_1201",DoubleType,true),StructField("component_1202",DoubleType,true),StructField("component_1203",DoubleType,true),StructField("component_1204",DoubleType,true),StructField("component_1205",DoubleType,true),StructField("component_1206",DoubleType,true),StructField("component_1207",DoubleType,true),StructField("component_1208",DoubleType,true),StructField("component_1209",DoubleType,true),StructField("component_1210",DoubleType,true),StructField("component_1211",DoubleType,true),StructField("component_1212",DoubleType,true),StructField("component_1213",DoubleType,true),StructField("component_1214",DoubleType,true),StructField("component_1215",DoubleType,true),StructField("component_1216",DoubleType,true),StructField("component_1217",DoubleType,true),StructField("component_1218",DoubleType,true),StructField("component_1219",DoubleType,true),StructField("component_1220",DoubleType,true),StructField("component_1221",DoubleType,true),StructField("component_1222",DoubleType,true),StructField("component_1223",DoubleType,true),StructField("component_1224",DoubleType,true),StructField("component_1225",DoubleType,true),StructField("component_1226",DoubleType,true),StructField("component_1227",DoubleType,true),StructField("component_1228",DoubleType,true),StructField("component_1229",DoubleType,true),StructField("component_1230",DoubleType,true),StructField("component_1231",DoubleType,true),StructField("component_1232",DoubleType,true),StructField("component_1233",DoubleType,true),StructField("component_1234",DoubleType,true),StructField("component_1235",DoubleType,true),StructField("component_1236",DoubleType,true),StructField("component_1237",DoubleType,true),StructField("component_1238",DoubleType,true),StructField("component_1239",DoubleType,true),StructField("component_1240",DoubleType,true),StructField("component_1241",DoubleType,true),StructField("component_1242",DoubleType,true),StructField("component_1243",DoubleType,true),StructField("component_1244",DoubleType,true),StructField("component_1245",DoubleType,true),StructField("component_1246",DoubleType,true),StructField("component_1247",DoubleType,true),StructField("component_1248",DoubleType,true),StructField("component_1249",DoubleType,true),StructField("component_1250",DoubleType,true),StructField("component_1251",DoubleType,true),StructField("component_1252",DoubleType,true),StructField("component_1253",DoubleType,true),StructField("component_1254",DoubleType,true),StructField("component_1255",DoubleType,true),StructField("component_1256",DoubleType,true),StructField("component_1257",DoubleType,true),StructField("component_1258",DoubleType,true),StructField("component_1259",DoubleType,true),StructField("component_1260",DoubleType,true),StructField("component_1261",DoubleType,true),StructField("component_1262",DoubleType,true),StructField("component_1263",DoubleType,true),StructField("component_1264",DoubleType,true),StructField("component_1265",DoubleType,true),StructField("component_1266",DoubleType,true),StructField("component_1267",DoubleType,true),StructField("component_1268",DoubleType,true),StructField("component_1269",DoubleType,true),StructField("component_1270",DoubleType,true),StructField("component_1271",DoubleType,true),StructField("component_1272",DoubleType,true),StructField("component_1273",DoubleType,true),StructField("component_1274",DoubleType,true),StructField("component_1275",DoubleType,true),StructField("component_1276",DoubleType,true),StructField("component_1277",DoubleType,true),StructField("component_1278",DoubleType,true),StructField("component_1279",DoubleType,true),StructField("component_1280",DoubleType,true),StructField("component_1281",DoubleType,true),StructField("component_1282",DoubleType,true),StructField("component_1283",DoubleType,true),StructField("component_1284",DoubleType,true),StructField("component_1285",DoubleType,true),StructField("component_1286",DoubleType,true),StructField("component_1287",DoubleType,true),StructField("component_1288",DoubleType,true),StructField("component_1289",DoubleType,true),StructField("component_1290",DoubleType,true),StructField("component_1291",DoubleType,true),StructField("component_1292",DoubleType,true),StructField("component_1293",DoubleType,true),StructField("component_1294",DoubleType,true),StructField("component_1295",DoubleType,true),StructField("component_1296",DoubleType,true),StructField("component_1297",DoubleType,true),StructField("component_1298",DoubleType,true),StructField("component_1299",DoubleType,true),StructField("component_1300",DoubleType,true),StructField("component_1301",DoubleType,true),StructField("component_1302",DoubleType,true),StructField("component_1303",DoubleType,true),StructField("component_1304",DoubleType,true),StructField("component_1305",DoubleType,true),StructField("component_1306",DoubleType,true),StructField("component_1307",DoubleType,true),StructField("component_1308",DoubleType,true),StructField("component_1309",DoubleType,true),StructField("component_1310",DoubleType,true),StructField("component_1311",DoubleType,true),StructField("component_1312",DoubleType,true),StructField("component_1313",DoubleType,true),StructField("component_1314",DoubleType,true),StructField("component_1315",DoubleType,true),StructField("component_1316",DoubleType,true),StructField("component_1317",DoubleType,true),StructField("component_1318",DoubleType,true),StructField("component_1319",DoubleType,true),StructField("component_1320",DoubleType,true),StructField("component_1321",DoubleType,true),StructField("component_1322",DoubleType,true),StructField("component_1323",DoubleType,true),StructField("component_1324",DoubleType,true),StructField("component_1325",DoubleType,true),StructField("component_1326",DoubleType,true),StructField("component_1327",DoubleType,true),StructField("component_1328",DoubleType,true),StructField("component_1329",DoubleType,true),StructField("component_1330",DoubleType,true),StructField("component_1331",DoubleType,true),StructField("component_1332",DoubleType,true),StructField("component_1333",DoubleType,true),StructField("component_1334",DoubleType,true),StructField("component_1335",DoubleType,true),StructField("component_1336",DoubleType,true),StructField("component_1337",DoubleType,true),StructField("component_1338",DoubleType,true),StructField("component_1339",DoubleType,true),StructField("component_1340",DoubleType,true),StructField("component_1341",DoubleType,true),StructField("component_1342",DoubleType,true),StructField("component_1343",DoubleType,true),StructField("component_1344",DoubleType,true),StructField("component_1345",DoubleType,true),StructField("component_1346",DoubleType,true),StructField("component_1347",DoubleType,true),StructField("component_1348",DoubleType,true),StructField("component_1349",DoubleType,true),StructField("component_1350",DoubleType,true),StructField("component_1351",DoubleType,true),StructField("component_1352",DoubleType,true),StructField("component_1353",DoubleType,true),StructField("component_1354",DoubleType,true),StructField("component_1355",DoubleType,true),StructField("component_1356",DoubleType,true),StructField("component_1357",DoubleType,true),StructField("component_1358",DoubleType,true),StructField("component_1359",DoubleType,true),StructField("component_1360",DoubleType,true),StructField("component_1361",DoubleType,true),StructField("component_1362",DoubleType,true),StructField("component_1363",DoubleType,true),StructField("component_1364",DoubleType,true),StructField("component_1365",DoubleType,true),StructField("component_1366",DoubleType,true),StructField("component_1367",DoubleType,true),StructField("component_1368",DoubleType,true),StructField("component_1369",DoubleType,true),StructField("component_1370",DoubleType,true),StructField("component_1371",DoubleType,true),StructField("component_1372",DoubleType,true),StructField("component_1373",DoubleType,true),StructField("component_1374",DoubleType,true),StructField("component_1375",DoubleType,true),StructField("component_1376",DoubleType,true),StructField("component_1377",DoubleType,true),StructField("component_1378",DoubleType,true),StructField("component_1379",DoubleType,true),StructField("component_1380",DoubleType,true),StructField("component_1381",DoubleType,true),StructField("component_1382",DoubleType,true),StructField("component_1383",DoubleType,true),StructField("component_1384",DoubleType,true),StructField("component_1385",DoubleType,true),StructField("component_1386",DoubleType,true),StructField("component_1387",DoubleType,true),StructField("component_1388",DoubleType,true),StructField("component_1389",DoubleType,true),StructField("component_1390",DoubleType,true),StructField("component_1391",DoubleType,true),StructField("component_1392",DoubleType,true),StructField("component_1393",DoubleType,true),StructField("component_1394",DoubleType,true),StructField("component_1395",DoubleType,true),StructField("component_1396",DoubleType,true),StructField("component_1397",DoubleType,true),StructField("component_1398",DoubleType,true),StructField("component_1399",DoubleType,true),StructField("component_1400",DoubleType,true),StructField("component_1401",DoubleType,true),StructField("component_1402",DoubleType,true),StructField("component_1403",DoubleType,true),StructField("component_1404",DoubleType,true),StructField("component_1405",DoubleType,true),StructField("component_1406",DoubleType,true),StructField("component_1407",DoubleType,true),StructField("component_1408",DoubleType,true),StructField("component_1409",DoubleType,true),StructField("component_1410",DoubleType,true),StructField("component_1411",DoubleType,true),StructField("component_1412",DoubleType,true),StructField("component_1413",DoubleType,true),StructField("component_1414",DoubleType,true),StructField("component_1415",DoubleType,true),StructField("component_1416",DoubleType,true),StructField("component_1417",DoubleType,true),StructField("component_1418",DoubleType,true),StructField("component_1419",DoubleType,true),StructField("component_1420",DoubleType,true),StructField("component_1421",DoubleType,true),StructField("component_1422",DoubleType,true),StructField("component_1423",DoubleType,true),StructField("component_1424",DoubleType,true),StructField("component_1425",DoubleType,true),StructField("component_1426",DoubleType,true),StructField("component_1427",DoubleType,true),StructField("component_1428",DoubleType,true),StructField("component_1429",DoubleType,true),StructField("component_1430",DoubleType,true),StructField("component_1431",DoubleType,true),StructField("component_1432",DoubleType,true),StructField("component_1433",DoubleType,true),StructField("component_1434",DoubleType,true),StructField("component_1435",DoubleType,true),StructField("component_1436",DoubleType,true),StructField("component_1437",DoubleType,true),StructField("component_1438",DoubleType,true),StructField("component_1439",DoubleType,true),StructField("component_1440",DoubleType,true),StructField(instanceName,StringType,true)))
# StructType(List(StructField("component_0",DoubleType,true),StructField("component_1",DoubleType,true),StructField("component_2",DoubleType,true),StructField("component_3",DoubleType,true),StructField("component_4",DoubleType,true),StructField("component_5",DoubleType,true),StructField("component_6",DoubleType,true),StructField("component_7",DoubleType,true),StructField("component_8",DoubleType,true),StructField("component_9",DoubleType,true),StructField("component_10",DoubleType,true),StructField("component_11",DoubleType,true),StructField("component_12",DoubleType,true),StructField("component_13",DoubleType,true),StructField("component_14",DoubleType,true),StructField("component_15",DoubleType,true),StructField("component_16",DoubleType,true),StructField("component_17",DoubleType,true),StructField("component_18",DoubleType,true),StructField("component_19",DoubleType,true),StructField("component_20",DoubleType,true),StructField("component_21",DoubleType,true),StructField("component_22",DoubleType,true),StructField("component_23",DoubleType,true),StructField("component_24",DoubleType,true),StructField("component_25",DoubleType,true),StructField("component_26",DoubleType,true),StructField("component_27",DoubleType,true),StructField("component_28",DoubleType,true),StructField("component_29",DoubleType,true),StructField("component_30",DoubleType,true),StructField("component_31",DoubleType,true),StructField("component_32",DoubleType,true),StructField("component_33",DoubleType,true),StructField("component_34",DoubleType,true),StructField("component_35",DoubleType,true),StructField("component_36",DoubleType,true),StructField("component_37",DoubleType,true),StructField("component_38",DoubleType,true),StructField("component_39",DoubleType,true),StructField("component_40",DoubleType,true),StructField("component_41",DoubleType,true),StructField("component_42",DoubleType,true),StructField("component_43",DoubleType,true),StructField("component_44",DoubleType,true),StructField("component_45",DoubleType,true),StructField("component_46",DoubleType,true),StructField("component_47",DoubleType,true),StructField("component_48",DoubleType,true),StructField("component_49",DoubleType,true),StructField("component_50",DoubleType,true),StructField("component_51",DoubleType,true),StructField("component_52",DoubleType,true),StructField("component_53",DoubleType,true),StructField("component_54",DoubleType,true),StructField("component_55",DoubleType,true),StructField("component_56",DoubleType,true),StructField("component_57",DoubleType,true),StructField("component_58",DoubleType,true),StructField("component_59",DoubleType,true),StructField("component_60",DoubleType,true),StructField("component_61",DoubleType,true),StructField("component_62",DoubleType,true),StructField("component_63",DoubleType,true),StructField("component_64",DoubleType,true),StructField("component_65",DoubleType,true),StructField("component_66",DoubleType,true),StructField("component_67",DoubleType,true),StructField("component_68",DoubleType,true),StructField("component_69",DoubleType,true),StructField("component_70",DoubleType,true),StructField("component_71",DoubleType,true),StructField("component_72",DoubleType,true),StructField("component_73",DoubleType,true),StructField("component_74",DoubleType,true),StructField("component_75",DoubleType,true),StructField("component_76",DoubleType,true),StructField("component_77",DoubleType,true),StructField("component_78",DoubleType,true),StructField("component_79",DoubleType,true),StructField("component_80",DoubleType,true),StructField("component_81",DoubleType,true),StructField("component_82",DoubleType,true),StructField("component_83",DoubleType,true),StructField("component_84",DoubleType,true),StructField("component_85",DoubleType,true),StructField("component_86",DoubleType,true),StructField("component_87",DoubleType,true),StructField("component_88",DoubleType,true),StructField("component_89",DoubleType,true),StructField("component_90",DoubleType,true),StructField("component_91",DoubleType,true),StructField("component_92",DoubleType,true),StructField("component_93",DoubleType,true),StructField("component_94",DoubleType,true),StructField("component_95",DoubleType,true),StructField("component_96",DoubleType,true),StructField("component_97",DoubleType,true),StructField("component_98",DoubleType,true),StructField("component_99",DoubleType,true),StructField("component_100",DoubleType,true),StructField("component_101",DoubleType,true),StructField("component_102",DoubleType,true),StructField("component_103",DoubleType,true),StructField("component_104",DoubleType,true),StructField("component_105",DoubleType,true),StructField("component_106",DoubleType,true),StructField("component_107",DoubleType,true),StructField("component_108",DoubleType,true),StructField("component_109",DoubleType,true),StructField("component_110",DoubleType,true),StructField("component_111",DoubleType,true),StructField("component_112",DoubleType,true),StructField("component_113",DoubleType,true),StructField("component_114",DoubleType,true),StructField("component_115",DoubleType,true),StructField("component_116",DoubleType,true),StructField("component_117",DoubleType,true),StructField("component_118",DoubleType,true),StructField("component_119",DoubleType,true),StructField("component_120",DoubleType,true),StructField("component_121",DoubleType,true),StructField("component_122",DoubleType,true),StructField("component_123",DoubleType,true),StructField("component_124",DoubleType,true),StructField("component_125",DoubleType,true),StructField("component_126",DoubleType,true),StructField("component_127",DoubleType,true),StructField("component_128",DoubleType,true),StructField("component_129",DoubleType,true),StructField("component_130",DoubleType,true),StructField("component_131",DoubleType,true),StructField("component_132",DoubleType,true),StructField("component_133",DoubleType,true),StructField("component_134",DoubleType,true),StructField("component_135",DoubleType,true),StructField("component_136",DoubleType,true),StructField("component_137",DoubleType,true),StructField("component_138",DoubleType,true),StructField("component_139",DoubleType,true),StructField("component_140",DoubleType,true),StructField("component_141",DoubleType,true),StructField("component_142",DoubleType,true),StructField("component_143",DoubleType,true),StructField("component_144",DoubleType,true),StructField("component_145",DoubleType,true),StructField("component_146",DoubleType,true),StructField("component_147",DoubleType,true),StructField("component_148",DoubleType,true),StructField("component_149",DoubleType,true),StructField("component_150",DoubleType,true),StructField("component_151",DoubleType,true),StructField("component_152",DoubleType,true),StructField("component_153",DoubleType,true),StructField("component_154",DoubleType,true),StructField("component_155",DoubleType,true),StructField("component_156",DoubleType,true),StructField("component_157",DoubleType,true),StructField("component_158",DoubleType,true),StructField("component_159",DoubleType,true),StructField("component_160",DoubleType,true),StructField("component_161",DoubleType,true),StructField("component_162",DoubleType,true),StructField("component_163",DoubleType,true),StructField("component_164",DoubleType,true),StructField("component_165",DoubleType,true),StructField("component_166",DoubleType,true),StructField("component_167",DoubleType,true),StructField(instanceName,StringType,true)))
# StructType(List(StructField("component_0",DoubleType,true),StructField("component_1",DoubleType,true),StructField("component_2",DoubleType,true),StructField("component_3",DoubleType,true),StructField("component_4",DoubleType,true),StructField("component_5",DoubleType,true),StructField("component_6",DoubleType,true),StructField("component_7",DoubleType,true),StructField("component_8",DoubleType,true),StructField("component_9",DoubleType,true),StructField("component_10",DoubleType,true),StructField("component_11",DoubleType,true),StructField("component_12",DoubleType,true),StructField("component_13",DoubleType,true),StructField("component_14",DoubleType,true),StructField("component_15",DoubleType,true),StructField("component_16",DoubleType,true),StructField("component_17",DoubleType,true),StructField("component_18",DoubleType,true),StructField("component_19",DoubleType,true),StructField("component_20",DoubleType,true),StructField("component_21",DoubleType,true),StructField("component_22",DoubleType,true),StructField("component_23",DoubleType,true),StructField("component_24",DoubleType,true),StructField("component_25",DoubleType,true),StructField("component_26",DoubleType,true),StructField("component_27",DoubleType,true),StructField("component_28",DoubleType,true),StructField("component_29",DoubleType,true),StructField("component_30",DoubleType,true),StructField("component_31",DoubleType,true),StructField("component_32",DoubleType,true),StructField("component_33",DoubleType,true),StructField("component_34",DoubleType,true),StructField("component_35",DoubleType,true),StructField("component_36",DoubleType,true),StructField("component_37",DoubleType,true),StructField("component_38",DoubleType,true),StructField("component_39",DoubleType,true),StructField("component_40",DoubleType,true),StructField("component_41",DoubleType,true),StructField("component_42",DoubleType,true),StructField("component_43",DoubleType,true),StructField("component_44",DoubleType,true),StructField("component_45",DoubleType,true),StructField("component_46",DoubleType,true),StructField("component_47",DoubleType,true),StructField("component_48",DoubleType,true),StructField("component_49",DoubleType,true),StructField("component_50",DoubleType,true),StructField("component_51",DoubleType,true),StructField("component_52",DoubleType,true),StructField("component_53",DoubleType,true),StructField("component_54",DoubleType,true),StructField("component_55",DoubleType,true),StructField("component_56",DoubleType,true),StructField("component_57",DoubleType,true),StructField("component_58",DoubleType,true),StructField("component_59",DoubleType,true),StructField("component_60",DoubleType,true),StructField("component_61",DoubleType,true),StructField("component_62",DoubleType,true),StructField("component_63",DoubleType,true),StructField("component_64",DoubleType,true),StructField("component_65",DoubleType,true),StructField("component_66",DoubleType,true),StructField("component_67",DoubleType,true),StructField("component_68",DoubleType,true),StructField("component_69",DoubleType,true),StructField("component_70",DoubleType,true),StructField("component_71",DoubleType,true),StructField("component_72",DoubleType,true),StructField("component_73",DoubleType,true),StructField("component_74",DoubleType,true),StructField("component_75",DoubleType,true),StructField("component_76",DoubleType,true),StructField("component_77",DoubleType,true),StructField("component_78",DoubleType,true),StructField("component_79",DoubleType,true),StructField("component_80",DoubleType,true),StructField("component_81",DoubleType,true),StructField("component_82",DoubleType,true),StructField("component_83",DoubleType,true),StructField("component_84",DoubleType,true),StructField("component_85",DoubleType,true),StructField("component_86",DoubleType,true),StructField("component_87",DoubleType,true),StructField("component_88",DoubleType,true),StructField("component_89",DoubleType,true),StructField("component_90",DoubleType,true),StructField("component_91",DoubleType,true),StructField("component_92",DoubleType,true),StructField("component_93",DoubleType,true),StructField("component_94",DoubleType,true),StructField("component_95",DoubleType,true),StructField("component_96",DoubleType,true),StructField("component_97",DoubleType,true),StructField("component_98",DoubleType,true),StructField("component_99",DoubleType,true),StructField("component_100",DoubleType,true),StructField("component_101",DoubleType,true),StructField("component_102",DoubleType,true),StructField("component_103",DoubleType,true),StructField("component_104",DoubleType,true),StructField("component_105",DoubleType,true),StructField("component_106",DoubleType,true),StructField("component_107",DoubleType,true),StructField("component_108",DoubleType,true),StructField("component_109",DoubleType,true),StructField("component_110",DoubleType,true),StructField("component_111",DoubleType,true),StructField("component_112",DoubleType,true),StructField("component_113",DoubleType,true),StructField("component_114",DoubleType,true),StructField("component_115",DoubleType,true),StructField("component_116",DoubleType,true),StructField("component_117",DoubleType,true),StructField("component_118",DoubleType,true),StructField("component_119",DoubleType,true),StructField("component_120",DoubleType,true),StructField("component_121",DoubleType,true),StructField("component_122",DoubleType,true),StructField("component_123",DoubleType,true),StructField("component_124",DoubleType,true),StructField("component_125",DoubleType,true),StructField("component_126",DoubleType,true),StructField("component_127",DoubleType,true),StructField("component_128",DoubleType,true),StructField("component_129",DoubleType,true),StructField("component_130",DoubleType,true),StructField("component_131",DoubleType,true),StructField("component_132",DoubleType,true),StructField("component_133",DoubleType,true),StructField("component_134",DoubleType,true),StructField("component_135",DoubleType,true),StructField("component_136",DoubleType,true),StructField("component_137",DoubleType,true),StructField("component_138",DoubleType,true),StructField("component_139",DoubleType,true),StructField("component_140",DoubleType,true),StructField("component_141",DoubleType,true),StructField("component_142",DoubleType,true),StructField("component_143",DoubleType,true),StructField("component_144",DoubleType,true),StructField("component_145",DoubleType,true),StructField("component_146",DoubleType,true),StructField("component_147",DoubleType,true),StructField("component_148",DoubleType,true),StructField("component_149",DoubleType,true),StructField("component_150",DoubleType,true),StructField("component_151",DoubleType,true),StructField("component_152",DoubleType,true),StructField("component_153",DoubleType,true),StructField("component_154",DoubleType,true),StructField("component_155",DoubleType,true),StructField("component_156",DoubleType,true),StructField("component_157",DoubleType,true),StructField("component_158",DoubleType,true),StructField("component_159",DoubleType,true),StructField("component_160",DoubleType,true),StructField("component_161",DoubleType,true),StructField("component_162",DoubleType,true),StructField("component_163",DoubleType,true),StructField("component_164",DoubleType,true),StructField("component_165",DoubleType,true),StructField("component_166",DoubleType,true),StructField("component_167",DoubleType,true),StructField("component_168",DoubleType,true),StructField("component_169",DoubleType,true),StructField("component_170",DoubleType,true),StructField("component_171",DoubleType,true),StructField("component_172",DoubleType,true),StructField("component_173",DoubleType,true),StructField("component_174",DoubleType,true),StructField("component_175",DoubleType,true),StructField("component_176",DoubleType,true),StructField("component_177",DoubleType,true),StructField("component_178",DoubleType,true),StructField("component_179",DoubleType,true),StructField("component_180",DoubleType,true),StructField("component_181",DoubleType,true),StructField("component_182",DoubleType,true),StructField("component_183",DoubleType,true),StructField("component_184",DoubleType,true),StructField("component_185",DoubleType,true),StructField("component_186",DoubleType,true),StructField("component_187",DoubleType,true),StructField("component_188",DoubleType,true),StructField("component_189",DoubleType,true),StructField("component_190",DoubleType,true),StructField("component_191",DoubleType,true),StructField("component_192",DoubleType,true),StructField("component_193",DoubleType,true),StructField("component_194",DoubleType,true),StructField("component_195",DoubleType,true),StructField("component_196",DoubleType,true),StructField("component_197",DoubleType,true),StructField("component_198",DoubleType,true),StructField("component_199",DoubleType,true),StructField("component_200",DoubleType,true),StructField("component_201",DoubleType,true),StructField("component_202",DoubleType,true),StructField("component_203",DoubleType,true),StructField("component_204",DoubleType,true),StructField("component_205",DoubleType,true),StructField("component_206",DoubleType,true),StructField("component_207",DoubleType,true),StructField("component_208",DoubleType,true),StructField("component_209",DoubleType,true),StructField("component_210",DoubleType,true),StructField("component_211",DoubleType,true),StructField("component_212",DoubleType,true),StructField("component_213",DoubleType,true),StructField("component_214",DoubleType,true),StructField("component_215",DoubleType,true),StructField("component_216",DoubleType,true),StructField("component_217",DoubleType,true),StructField("component_218",DoubleType,true),StructField("component_219",DoubleType,true),StructField("component_220",DoubleType,true),StructField("component_221",DoubleType,true),StructField("component_222",DoubleType,true),StructField("component_223",DoubleType,true),StructField("component_224",DoubleType,true),StructField("component_225",DoubleType,true),StructField("component_226",DoubleType,true),StructField("component_227",DoubleType,true),StructField("component_228",DoubleType,true),StructField("component_229",DoubleType,true),StructField("component_230",DoubleType,true),StructField("component_231",DoubleType,true),StructField("component_232",DoubleType,true),StructField("component_233",DoubleType,true),StructField("component_234",DoubleType,true),StructField("component_235",DoubleType,true),StructField("component_236",DoubleType,true),StructField("component_237",DoubleType,true),StructField("component_238",DoubleType,true),StructField("component_239",DoubleType,true),StructField("component_240",DoubleType,true),StructField("component_241",DoubleType,true),StructField("component_242",DoubleType,true),StructField("component_243",DoubleType,true),StructField("component_244",DoubleType,true),StructField("component_245",DoubleType,true),StructField("component_246",DoubleType,true),StructField("component_247",DoubleType,true),StructField("component_248",DoubleType,true),StructField("component_249",DoubleType,true),StructField("component_250",DoubleType,true),StructField("component_251",DoubleType,true),StructField("component_252",DoubleType,true),StructField("component_253",DoubleType,true),StructField("component_254",DoubleType,true),StructField("component_255",DoubleType,true),StructField("component_256",DoubleType,true),StructField("component_257",DoubleType,true),StructField("component_258",DoubleType,true),StructField("component_259",DoubleType,true),StructField("component_260",DoubleType,true),StructField("component_261",DoubleType,true),StructField("component_262",DoubleType,true),StructField("component_263",DoubleType,true),StructField("component_264",DoubleType,true),StructField("component_265",DoubleType,true),StructField("component_266",DoubleType,true),StructField("component_267",DoubleType,true),StructField("component_268",DoubleType,true),StructField("component_269",DoubleType,true),StructField("component_270",DoubleType,true),StructField("component_271",DoubleType,true),StructField("component_272",DoubleType,true),StructField("component_273",DoubleType,true),StructField("component_274",DoubleType,true),StructField("component_275",DoubleType,true),StructField("component_276",DoubleType,true),StructField("component_277",DoubleType,true),StructField("component_278",DoubleType,true),StructField("component_279",DoubleType,true),StructField("component_280",DoubleType,true),StructField("component_281",DoubleType,true),StructField("component_282",DoubleType,true),StructField("component_283",DoubleType,true),StructField("component_284",DoubleType,true),StructField("component_285",DoubleType,true),StructField("component_286",DoubleType,true),StructField("component_287",DoubleType,true),StructField("component_288",DoubleType,true),StructField("component_289",DoubleType,true),StructField("component_290",DoubleType,true),StructField("component_291",DoubleType,true),StructField("component_292",DoubleType,true),StructField("component_293",DoubleType,true),StructField("component_294",DoubleType,true),StructField("component_295",DoubleType,true),StructField("component_296",DoubleType,true),StructField("component_297",DoubleType,true),StructField("component_298",DoubleType,true),StructField("component_299",DoubleType,true),StructField("component_300",DoubleType,true),StructField("component_301",DoubleType,true),StructField("component_302",DoubleType,true),StructField("component_303",DoubleType,true),StructField("component_304",DoubleType,true),StructField("component_305",DoubleType,true),StructField("component_306",DoubleType,true),StructField("component_307",DoubleType,true),StructField("component_308",DoubleType,true),StructField("component_309",DoubleType,true),StructField("component_310",DoubleType,true),StructField("component_311",DoubleType,true),StructField("component_312",DoubleType,true),StructField("component_313",DoubleType,true),StructField("component_314",DoubleType,true),StructField("component_315",DoubleType,true),StructField("component_316",DoubleType,true),StructField("component_317",DoubleType,true),StructField("component_318",DoubleType,true),StructField("component_319",DoubleType,true),StructField("component_320",DoubleType,true),StructField("component_321",DoubleType,true),StructField("component_322",DoubleType,true),StructField("component_323",DoubleType,true),StructField("component_324",DoubleType,true),StructField("component_325",DoubleType,true),StructField("component_326",DoubleType,true),StructField("component_327",DoubleType,true),StructField("component_328",DoubleType,true),StructField("component_329",DoubleType,true),StructField("component_330",DoubleType,true),StructField("component_331",DoubleType,true),StructField("component_332",DoubleType,true),StructField("component_333",DoubleType,true),StructField("component_334",DoubleType,true),StructField("component_335",DoubleType,true),StructField("component_336",DoubleType,true),StructField("component_337",DoubleType,true),StructField("component_338",DoubleType,true),StructField("component_339",DoubleType,true),StructField("component_340",DoubleType,true),StructField("component_341",DoubleType,true),StructField("component_342",DoubleType,true),StructField("component_343",DoubleType,true),StructField("component_344",DoubleType,true),StructField("component_345",DoubleType,true),StructField("component_346",DoubleType,true),StructField("component_347",DoubleType,true),StructField("component_348",DoubleType,true),StructField("component_349",DoubleType,true),StructField("component_350",DoubleType,true),StructField("component_351",DoubleType,true),StructField("component_352",DoubleType,true),StructField("component_353",DoubleType,true),StructField("component_354",DoubleType,true),StructField("component_355",DoubleType,true),StructField("component_356",DoubleType,true),StructField("component_357",DoubleType,true),StructField("component_358",DoubleType,true),StructField("component_359",DoubleType,true),StructField("component_360",DoubleType,true),StructField("component_361",DoubleType,true),StructField("component_362",DoubleType,true),StructField("component_363",DoubleType,true),StructField("component_364",DoubleType,true),StructField("component_365",DoubleType,true),StructField("component_366",DoubleType,true),StructField("component_367",DoubleType,true),StructField("component_368",DoubleType,true),StructField("component_369",DoubleType,true),StructField("component_370",DoubleType,true),StructField("component_371",DoubleType,true),StructField("component_372",DoubleType,true),StructField("component_373",DoubleType,true),StructField("component_374",DoubleType,true),StructField("component_375",DoubleType,true),StructField("component_376",DoubleType,true),StructField("component_377",DoubleType,true),StructField("component_378",DoubleType,true),StructField("component_379",DoubleType,true),StructField("component_380",DoubleType,true),StructField("component_381",DoubleType,true),StructField("component_382",DoubleType,true),StructField("component_383",DoubleType,true),StructField("component_384",DoubleType,true),StructField("component_385",DoubleType,true),StructField("component_386",DoubleType,true),StructField("component_387",DoubleType,true),StructField("component_388",DoubleType,true),StructField("component_389",DoubleType,true),StructField("component_390",DoubleType,true),StructField("component_391",DoubleType,true),StructField("component_392",DoubleType,true),StructField("component_393",DoubleType,true),StructField("component_394",DoubleType,true),StructField("component_395",DoubleType,true),StructField("component_396",DoubleType,true),StructField("component_397",DoubleType,true),StructField("component_398",DoubleType,true),StructField("component_399",DoubleType,true),StructField("component_400",DoubleType,true),StructField("component_401",DoubleType,true),StructField("component_402",DoubleType,true),StructField("component_403",DoubleType,true),StructField("component_404",DoubleType,true),StructField("component_405",DoubleType,true),StructField("component_406",DoubleType,true),StructField("component_407",DoubleType,true),StructField("component_408",DoubleType,true),StructField("component_409",DoubleType,true),StructField("component_410",DoubleType,true),StructField("component_411",DoubleType,true),StructField("component_412",DoubleType,true),StructField("component_413",DoubleType,true),StructField("component_414",DoubleType,true),StructField("component_415",DoubleType,true),StructField("component_416",DoubleType,true),StructField("component_417",DoubleType,true),StructField("component_418",DoubleType,true),StructField("component_419",DoubleType,true),StructField(instanceName,StringType,true)))
# StructType(List(StructField("component_1",DoubleType,true),StructField("component_2",DoubleType,true),StructField("component_3",DoubleType,true),StructField("component_4",DoubleType,true),StructField("component_5",DoubleType,true),StructField("component_6",DoubleType,true),StructField("component_7",DoubleType,true),StructField("component_8",DoubleType,true),StructField("component_9",DoubleType,true),StructField("component_10",DoubleType,true),StructField("component_11",DoubleType,true),StructField("component_12",DoubleType,true),StructField("component_13",DoubleType,true),StructField("component_14",DoubleType,true),StructField("component_15",DoubleType,true),StructField("component_16",DoubleType,true),StructField("component_17",DoubleType,true),StructField("component_18",DoubleType,true),StructField("component_19",DoubleType,true),StructField("component_20",DoubleType,true),StructField("component_21",DoubleType,true),StructField("component_22",DoubleType,true),StructField("component_23",DoubleType,true),StructField("component_24",DoubleType,true),StructField("component_25",DoubleType,true),StructField("component_26",DoubleType,true),StructField("component_27",DoubleType,true),StructField("component_28",DoubleType,true),StructField("component_29",DoubleType,true),StructField("component_30",DoubleType,true),StructField("component_31",DoubleType,true),StructField("component_32",DoubleType,true),StructField("component_33",DoubleType,true),StructField("component_34",DoubleType,true),StructField("component_35",DoubleType,true),StructField("component_36",DoubleType,true),StructField("component_37",DoubleType,true),StructField("component_38",DoubleType,true),StructField("component_39",DoubleType,true),StructField("component_40",DoubleType,true),StructField("component_41",DoubleType,true),StructField("component_42",DoubleType,true),StructField("component_43",DoubleType,true),StructField("component_44",DoubleType,true),StructField("component_45",DoubleType,true),StructField("component_46",DoubleType,true),StructField("component_47",DoubleType,true),StructField("component_48",DoubleType,true),StructField("component_49",DoubleType,true),StructField("component_50",DoubleType,true),StructField("component_51",DoubleType,true),StructField("component_52",DoubleType,true),StructField("component_53",DoubleType,true),StructField("component_54",DoubleType,true),StructField("component_55",DoubleType,true),StructField("component_56",DoubleType,true),StructField("component_57",DoubleType,true),StructField("component_58",DoubleType,true),StructField("component_59",DoubleType,true),StructField("component_60",DoubleType,true),StructField("component_61",DoubleType,true),StructField("component_62",DoubleType,true),StructField("component_63",DoubleType,true),StructField("component_64",DoubleType,true),StructField("component_65",DoubleType,true),StructField("component_66",DoubleType,true),StructField("component_67",DoubleType,true),StructField("component_68",DoubleType,true),StructField("component_69",DoubleType,true),StructField("component_70",DoubleType,true),StructField("component_71",DoubleType,true),StructField("component_72",DoubleType,true),StructField("component_73",DoubleType,true),StructField("component_74",DoubleType,true),StructField("component_75",DoubleType,true),StructField("component_76",DoubleType,true),StructField("component_77",DoubleType,true),StructField("component_78",DoubleType,true),StructField("component_79",DoubleType,true),StructField("component_80",DoubleType,true),StructField("component_81",DoubleType,true),StructField("component_82",DoubleType,true),StructField("component_83",DoubleType,true),StructField("component_84",DoubleType,true),StructField("component_85",DoubleType,true),StructField("component_86",DoubleType,true),StructField("component_87",DoubleType,true),StructField("component_88",DoubleType,true),StructField("component_89",DoubleType,true),StructField("component_90",DoubleType,true),StructField("component_91",DoubleType,true),StructField("component_92",DoubleType,true),StructField("component_93",DoubleType,true),StructField("component_94",DoubleType,true),StructField("component_95",DoubleType,true),StructField("component_96",DoubleType,true),StructField("component_97",DoubleType,true),StructField("component_98",DoubleType,true),StructField("component_99",DoubleType,true),StructField("component_100",DoubleType,true),StructField("component_101",DoubleType,true),StructField("component_102",DoubleType,true),StructField("component_103",DoubleType,true),StructField("component_104",DoubleType,true),StructField("component_105",DoubleType,true),StructField("component_106",DoubleType,true),StructField("component_107",DoubleType,true),StructField("component_108",DoubleType,true),StructField("component_109",DoubleType,true),StructField("component_110",DoubleType,true),StructField("component_111",DoubleType,true),StructField("component_112",DoubleType,true),StructField("component_113",DoubleType,true),StructField("component_114",DoubleType,true),StructField("component_115",DoubleType,true),StructField("component_116",DoubleType,true),StructField("component_117",DoubleType,true),StructField("component_118",DoubleType,true),StructField("component_119",DoubleType,true),StructField("component_120",DoubleType,true),StructField("component_121",DoubleType,true),StructField("component_122",DoubleType,true),StructField("component_123",DoubleType,true),StructField("component_124",DoubleType,true),StructField("component_125",DoubleType,true),StructField("component_126",DoubleType,true),StructField("component_127",DoubleType,true),StructField("component_128",DoubleType,true),StructField("component_129",DoubleType,true),StructField("component_130",DoubleType,true),StructField("component_131",DoubleType,true),StructField("component_132",DoubleType,true),StructField("component_133",DoubleType,true),StructField("component_134",DoubleType,true),StructField("component_135",DoubleType,true),StructField("component_136",DoubleType,true),StructField("component_137",DoubleType,true),StructField("component_138",DoubleType,true),StructField("component_139",DoubleType,true),StructField("component_140",DoubleType,true),StructField("component_141",DoubleType,true),StructField("component_142",DoubleType,true),StructField("component_143",DoubleType,true),StructField("component_144",DoubleType,true),StructField("component_145",DoubleType,true),StructField("component_146",DoubleType,true),StructField("component_147",DoubleType,true),StructField("component_148",DoubleType,true),StructField("component_149",DoubleType,true),StructField("component_150",DoubleType,true),StructField("component_151",DoubleType,true),StructField("component_152",DoubleType,true),StructField("component_153",DoubleType,true),StructField("component_154",DoubleType,true),StructField("component_155",DoubleType,true),StructField("component_156",DoubleType,true),StructField("component_157",DoubleType,true),StructField("component_158",DoubleType,true),StructField("component_159",DoubleType,true),StructField("component_160",DoubleType,true),StructField("component_161",DoubleType,true),StructField("component_162",DoubleType,true),StructField("component_163",DoubleType,true),StructField("component_164",DoubleType,true),StructField("component_165",DoubleType,true),StructField("component_166",DoubleType,true),StructField("component_167",DoubleType,true),StructField("component_168",DoubleType,true),StructField("component_169",DoubleType,true),StructField("component_170",DoubleType,true),StructField("component_171",DoubleType,true),StructField("component_172",DoubleType,true),StructField("component_173",DoubleType,true),StructField("component_174",DoubleType,true),StructField("component_175",DoubleType,true),StructField("component_176",DoubleType,true),StructField("component_177",DoubleType,true),StructField("component_178",DoubleType,true),StructField("component_179",DoubleType,true),StructField("component_180",DoubleType,true),StructField("component_181",DoubleType,true),StructField("component_182",DoubleType,true),StructField("component_183",DoubleType,true),StructField("component_184",DoubleType,true),StructField("component_185",DoubleType,true),StructField("component_186",DoubleType,true),StructField("component_187",DoubleType,true),StructField("component_188",DoubleType,true),StructField("component_189",DoubleType,true),StructField("component_190",DoubleType,true),StructField("component_191",DoubleType,true),StructField("component_192",DoubleType,true),StructField("component_193",DoubleType,true),StructField("component_194",DoubleType,true),StructField("component_195",DoubleType,true),StructField("component_196",DoubleType,true),StructField("component_197",DoubleType,true),StructField("component_198",DoubleType,true),StructField("component_199",DoubleType,true),StructField("component_200",DoubleType,true),StructField("component_201",DoubleType,true),StructField("component_202",DoubleType,true),StructField("component_203",DoubleType,true),StructField("component_204",DoubleType,true),StructField("component_205",DoubleType,true),StructField("component_206",DoubleType,true),StructField("component_207",DoubleType,true),StructField("component_208",DoubleType,true),StructField("component_209",DoubleType,true),StructField("component_210",DoubleType,true),StructField("component_211",DoubleType,true),StructField("component_212",DoubleType,true),StructField("component_213",DoubleType,true),StructField("component_214",DoubleType,true),StructField("component_215",DoubleType,true),StructField("component_216",DoubleType,true),StructField("component_217",DoubleType,true),StructField("component_218",DoubleType,true),StructField("component_219",DoubleType,true),StructField("component_220",DoubleType,true),StructField("component_221",DoubleType,true),StructField("component_222",DoubleType,true),StructField("component_223",DoubleType,true),StructField("component_224",DoubleType,true),StructField("component_225",DoubleType,true),StructField("component_226",DoubleType,true),StructField("component_227",DoubleType,true),StructField("component_228",DoubleType,true),StructField("component_229",DoubleType,true),StructField("component_230",DoubleType,true),StructField("component_231",DoubleType,true),StructField("component_232",DoubleType,true),StructField("component_233",DoubleType,true),StructField("component_234",DoubleType,true),StructField("component_235",DoubleType,true),StructField("component_236",DoubleType,true),StructField("component_237",DoubleType,true),StructField("component_238",DoubleType,true),StructField("component_239",DoubleType,true),StructField("component_240",DoubleType,true),StructField("component_241",DoubleType,true),StructField("component_242",DoubleType,true),StructField("component_243",DoubleType,true),StructField("component_244",DoubleType,true),StructField("component_245",DoubleType,true),StructField("component_246",DoubleType,true),StructField("component_247",DoubleType,true),StructField("component_248",DoubleType,true),StructField("component_249",DoubleType,true),StructField("component_250",DoubleType,true),StructField("component_251",DoubleType,true),StructField("component_252",DoubleType,true),StructField("component_253",DoubleType,true),StructField("component_254",DoubleType,true),StructField("component_255",DoubleType,true),StructField("component_256",DoubleType,true),StructField("component_257",DoubleType,true),StructField("component_258",DoubleType,true),StructField("component_259",DoubleType,true),StructField("component_260",DoubleType,true),StructField("component_261",DoubleType,true),StructField("component_262",DoubleType,true),StructField("component_263",DoubleType,true),StructField("component_264",DoubleType,true),StructField("component_265",DoubleType,true),StructField("component_266",DoubleType,true),StructField("component_267",DoubleType,true),StructField("component_268",DoubleType,true),StructField("component_269",DoubleType,true),StructField("component_270",DoubleType,true),StructField("component_271",DoubleType,true),StructField("component_272",DoubleType,true),StructField("component_273",DoubleType,true),StructField("component_274",DoubleType,true),StructField("component_275",DoubleType,true),StructField("component_276",DoubleType,true),StructField("component_277",DoubleType,true),StructField("component_278",DoubleType,true),StructField("component_279",DoubleType,true),StructField("component_280",DoubleType,true),StructField("component_281",DoubleType,true),StructField("component_282",DoubleType,true),StructField("component_283",DoubleType,true),StructField("component_284",DoubleType,true),StructField("component_285",DoubleType,true),StructField("component_286",DoubleType,true),StructField("component_287",DoubleType,true),StructField("component_288",DoubleType,true),StructField("component_289",DoubleType,true),StructField("component_290",DoubleType,true),StructField("component_291",DoubleType,true),StructField("component_292",DoubleType,true),StructField("component_293",DoubleType,true),StructField("component_294",DoubleType,true),StructField("component_295",DoubleType,true),StructField("component_296",DoubleType,true),StructField("component_297",DoubleType,true),StructField("component_298",DoubleType,true),StructField("component_299",DoubleType,true),StructField("component_300",DoubleType,true),StructField("component_301",DoubleType,true),StructField("component_302",DoubleType,true),StructField("component_303",DoubleType,true),StructField("component_304",DoubleType,true),StructField("component_305",DoubleType,true),StructField("component_306",DoubleType,true),StructField("component_307",DoubleType,true),StructField("component_308",DoubleType,true),StructField("component_309",DoubleType,true),StructField("component_310",DoubleType,true),StructField("component_311",DoubleType,true),StructField("component_312",DoubleType,true),StructField("component_313",DoubleType,true),StructField("component_314",DoubleType,true),StructField("component_315",DoubleType,true),StructField("component_316",DoubleType,true),StructField("component_317",DoubleType,true),StructField("component_318",DoubleType,true),StructField("component_319",DoubleType,true),StructField("component_320",DoubleType,true),StructField("component_321",DoubleType,true),StructField("component_322",DoubleType,true),StructField("component_323",DoubleType,true),StructField("component_324",DoubleType,true),StructField("component_325",DoubleType,true),StructField("component_326",DoubleType,true),StructField("component_327",DoubleType,true),StructField("component_328",DoubleType,true),StructField("component_329",DoubleType,true),StructField("component_330",DoubleType,true),StructField("component_331",DoubleType,true),StructField("component_332",DoubleType,true),StructField("component_333",DoubleType,true),StructField("component_334",DoubleType,true),StructField("component_335",DoubleType,true),StructField("component_336",DoubleType,true),StructField("component_337",DoubleType,true),StructField("component_338",DoubleType,true),StructField("component_339",DoubleType,true),StructField("component_340",DoubleType,true),StructField("component_341",DoubleType,true),StructField("component_342",DoubleType,true),StructField("component_343",DoubleType,true),StructField("component_344",DoubleType,true),StructField("component_345",DoubleType,true),StructField("component_346",DoubleType,true),StructField("component_347",DoubleType,true),StructField("component_348",DoubleType,true),StructField("component_349",DoubleType,true),StructField("component_350",DoubleType,true),StructField("component_351",DoubleType,true),StructField("component_352",DoubleType,true),StructField("component_353",DoubleType,true),StructField("component_354",DoubleType,true),StructField("component_355",DoubleType,true),StructField("component_356",DoubleType,true),StructField("component_357",DoubleType,true),StructField("component_358",DoubleType,true),StructField("component_359",DoubleType,true),StructField("component_360",DoubleType,true),StructField("component_361",DoubleType,true),StructField("component_362",DoubleType,true),StructField("component_363",DoubleType,true),StructField("component_364",DoubleType,true),StructField("component_365",DoubleType,true),StructField("component_366",DoubleType,true),StructField("component_367",DoubleType,true),StructField("component_368",DoubleType,true),StructField("component_369",DoubleType,true),StructField("component_370",DoubleType,true),StructField("component_371",DoubleType,true),StructField("component_372",DoubleType,true),StructField("component_373",DoubleType,true),StructField("component_374",DoubleType,true),StructField("component_375",DoubleType,true),StructField("component_376",DoubleType,true),StructField("component_377",DoubleType,true),StructField("component_378",DoubleType,true),StructField("component_379",DoubleType,true),StructField("component_380",DoubleType,true),StructField("component_381",DoubleType,true),StructField("component_382",DoubleType,true),StructField("component_383",DoubleType,true),StructField("component_384",DoubleType,true),StructField("component_385",DoubleType,true),StructField("component_386",DoubleType,true),StructField("component_387",DoubleType,true),StructField("component_388",DoubleType,true),StructField("component_389",DoubleType,true),StructField("component_390",DoubleType,true),StructField("component_391",DoubleType,true),StructField("component_392",DoubleType,true),StructField("component_393",DoubleType,true),StructField("component_394",DoubleType,true),StructField("component_395",DoubleType,true),StructField("component_396",DoubleType,true),StructField("component_397",DoubleType,true),StructField("component_398",DoubleType,true),StructField("component_399",DoubleType,true),StructField("component_400",DoubleType,true),StructField("component_401",DoubleType,true),StructField("component_402",DoubleType,true),StructField("component_403",DoubleType,true),StructField("component_404",DoubleType,true),StructField("component_405",DoubleType,true),StructField("component_406",DoubleType,true),StructField("component_407",DoubleType,true),StructField("component_408",DoubleType,true),StructField("component_409",DoubleType,true),StructField("component_410",DoubleType,true),StructField("component_411",DoubleType,true),StructField("component_412",DoubleType,true),StructField("component_413",DoubleType,true),StructField("component_414",DoubleType,true),StructField("component_415",DoubleType,true),StructField("component_416",DoubleType,true),StructField("component_417",DoubleType,true),StructField("component_418",DoubleType,true),StructField("component_419",DoubleType,true),StructField("component_420",DoubleType,true),StructField("component_421",DoubleType,true),StructField("component_422",DoubleType,true),StructField("component_423",DoubleType,true),StructField("component_424",DoubleType,true),StructField("component_425",DoubleType,true),StructField("component_426",DoubleType,true),StructField("component_427",DoubleType,true),StructField("component_428",DoubleType,true),StructField("component_429",DoubleType,true),StructField("component_430",DoubleType,true),StructField("component_431",DoubleType,true),StructField("component_432",DoubleType,true),StructField("component_433",DoubleType,true),StructField("component_434",DoubleType,true),StructField("component_435",DoubleType,true),StructField("component_436",DoubleType,true),StructField("component_437",DoubleType,true),StructField("component_438",DoubleType,true),StructField("component_439",DoubleType,true),StructField("component_440",DoubleType,true),StructField("component_441",DoubleType,true),StructField("component_442",DoubleType,true),StructField("component_443",DoubleType,true),StructField("component_444",DoubleType,true),StructField("component_445",DoubleType,true),StructField("component_446",DoubleType,true),StructField("component_447",DoubleType,true),StructField("component_448",DoubleType,true),StructField("component_449",DoubleType,true),StructField("component_450",DoubleType,true),StructField("component_451",DoubleType,true),StructField("component_452",DoubleType,true),StructField("component_453",DoubleType,true),StructField("component_454",DoubleType,true),StructField("component_455",DoubleType,true),StructField("component_456",DoubleType,true),StructField("component_457",DoubleType,true),StructField("component_458",DoubleType,true),StructField("component_459",DoubleType,true),StructField("component_460",DoubleType,true),StructField("component_461",DoubleType,true),StructField("component_462",DoubleType,true),StructField("component_463",DoubleType,true),StructField("component_464",DoubleType,true),StructField("component_465",DoubleType,true),StructField("component_466",DoubleType,true),StructField("component_467",DoubleType,true),StructField("component_468",DoubleType,true),StructField("component_469",DoubleType,true),StructField("component_470",DoubleType,true),StructField("component_471",DoubleType,true),StructField("component_472",DoubleType,true),StructField("component_473",DoubleType,true),StructField("component_474",DoubleType,true),StructField("component_475",DoubleType,true),StructField("component_476",DoubleType,true),StructField("component_477",DoubleType,true),StructField("component_478",DoubleType,true),StructField("component_479",DoubleType,true),StructField("component_480",DoubleType,true),StructField("component_481",DoubleType,true),StructField("component_482",DoubleType,true),StructField("component_483",DoubleType,true),StructField("component_484",DoubleType,true),StructField("component_485",DoubleType,true),StructField("component_486",DoubleType,true),StructField("component_487",DoubleType,true),StructField("component_488",DoubleType,true),StructField("component_489",DoubleType,true),StructField("component_490",DoubleType,true),StructField("component_491",DoubleType,true),StructField("component_492",DoubleType,true),StructField("component_493",DoubleType,true),StructField("component_494",DoubleType,true),StructField("component_495",DoubleType,true),StructField("component_496",DoubleType,true),StructField("component_497",DoubleType,true),StructField("component_498",DoubleType,true),StructField("component_499",DoubleType,true),StructField("component_500",DoubleType,true),StructField("component_501",DoubleType,true),StructField("component_502",DoubleType,true),StructField("component_503",DoubleType,true),StructField("component_504",DoubleType,true),StructField("component_505",DoubleType,true),StructField("component_506",DoubleType,true),StructField("component_507",DoubleType,true),StructField("component_508",DoubleType,true),StructField("component_509",DoubleType,true),StructField("component_510",DoubleType,true),StructField("component_511",DoubleType,true),StructField("component_512",DoubleType,true),StructField("component_513",DoubleType,true),StructField("component_514",DoubleType,true),StructField("component_515",DoubleType,true),StructField("component_516",DoubleType,true),StructField("component_517",DoubleType,true),StructField("component_518",DoubleType,true),StructField("component_519",DoubleType,true),StructField("component_520",DoubleType,true),StructField("component_521",DoubleType,true),StructField("component_522",DoubleType,true),StructField("component_523",DoubleType,true),StructField("component_524",DoubleType,true),StructField("component_525",DoubleType,true),StructField("component_526",DoubleType,true),StructField("component_527",DoubleType,true),StructField("component_528",DoubleType,true),StructField("component_529",DoubleType,true),StructField("component_530",DoubleType,true),StructField("component_531",DoubleType,true),StructField("component_532",DoubleType,true),StructField("component_533",DoubleType,true),StructField("component_534",DoubleType,true),StructField("component_535",DoubleType,true),StructField("component_536",DoubleType,true),StructField("component_537",DoubleType,true),StructField("component_538",DoubleType,true),StructField("component_539",DoubleType,true),StructField("component_540",DoubleType,true),StructField("component_541",DoubleType,true),StructField("component_542",DoubleType,true),StructField("component_543",DoubleType,true),StructField("component_544",DoubleType,true),StructField("component_545",DoubleType,true),StructField("component_546",DoubleType,true),StructField("component_547",DoubleType,true),StructField("component_548",DoubleType,true),StructField("component_549",DoubleType,true),StructField("component_550",DoubleType,true),StructField("component_551",DoubleType,true),StructField("component_552",DoubleType,true),StructField("component_553",DoubleType,true),StructField("component_554",DoubleType,true),StructField("component_555",DoubleType,true),StructField("component_556",DoubleType,true),StructField("component_557",DoubleType,true),StructField("component_558",DoubleType,true),StructField("component_559",DoubleType,true),StructField("component_560",DoubleType,true),StructField("component_561",DoubleType,true),StructField("component_562",DoubleType,true),StructField("component_563",DoubleType,true),StructField("component_564",DoubleType,true),StructField("component_565",DoubleType,true),StructField("component_566",DoubleType,true),StructField("component_567",DoubleType,true),StructField("component_568",DoubleType,true),StructField("component_569",DoubleType,true),StructField("component_570",DoubleType,true),StructField("component_571",DoubleType,true),StructField("component_572",DoubleType,true),StructField("component_573",DoubleType,true),StructField("component_574",DoubleType,true),StructField("component_575",DoubleType,true),StructField("component_576",DoubleType,true),StructField("component_577",DoubleType,true),StructField("component_578",DoubleType,true),StructField("component_579",DoubleType,true),StructField("component_580",DoubleType,true),StructField("component_581",DoubleType,true),StructField("component_582",DoubleType,true),StructField("component_583",DoubleType,true),StructField("component_584",DoubleType,true),StructField("component_585",DoubleType,true),StructField("component_586",DoubleType,true),StructField("component_587",DoubleType,true),StructField("component_588",DoubleType,true),StructField("component_589",DoubleType,true),StructField("component_590",DoubleType,true),StructField("component_591",DoubleType,true),StructField("component_592",DoubleType,true),StructField("component_593",DoubleType,true),StructField("component_594",DoubleType,true),StructField("component_595",DoubleType,true),StructField("component_596",DoubleType,true),StructField("component_597",DoubleType,true),StructField("component_598",DoubleType,true),StructField("component_599",DoubleType,true),StructField("component_600",DoubleType,true),StructField("component_601",DoubleType,true),StructField("component_602",DoubleType,true),StructField("component_603",DoubleType,true),StructField("component_604",DoubleType,true),StructField("component_605",DoubleType,true),StructField("component_606",DoubleType,true),StructField("component_607",DoubleType,true),StructField("component_608",DoubleType,true),StructField("component_609",DoubleType,true),StructField("component_610",DoubleType,true),StructField("component_611",DoubleType,true),StructField("component_612",DoubleType,true),StructField("component_613",DoubleType,true),StructField("component_614",DoubleType,true),StructField("component_615",DoubleType,true),StructField("component_616",DoubleType,true),StructField("component_617",DoubleType,true),StructField("component_618",DoubleType,true),StructField("component_619",DoubleType,true),StructField("component_620",DoubleType,true),StructField("component_621",DoubleType,true),StructField("component_622",DoubleType,true),StructField("component_623",DoubleType,true),StructField("component_624",DoubleType,true),StructField("component_625",DoubleType,true),StructField("component_626",DoubleType,true),StructField("component_627",DoubleType,true),StructField("component_628",DoubleType,true),StructField("component_629",DoubleType,true),StructField("component_630",DoubleType,true),StructField("component_631",DoubleType,true),StructField("component_632",DoubleType,true),StructField("component_633",DoubleType,true),StructField("component_634",DoubleType,true),StructField("component_635",DoubleType,true),StructField("component_636",DoubleType,true),StructField("component_637",DoubleType,true),StructField("component_638",DoubleType,true),StructField("component_639",DoubleType,true),StructField("component_640",DoubleType,true),StructField("component_641",DoubleType,true),StructField("component_642",DoubleType,true),StructField("component_643",DoubleType,true),StructField("component_644",DoubleType,true),StructField("component_645",DoubleType,true),StructField("component_646",DoubleType,true),StructField("component_647",DoubleType,true),StructField("component_648",DoubleType,true),StructField("component_649",DoubleType,true),StructField("component_650",DoubleType,true),StructField("component_651",DoubleType,true),StructField("component_652",DoubleType,true),StructField("component_653",DoubleType,true),StructField("component_654",DoubleType,true),StructField("component_655",DoubleType,true),StructField("component_656",DoubleType,true),StructField("component_657",DoubleType,true),StructField("component_658",DoubleType,true),StructField("component_659",DoubleType,true),StructField("component_660",DoubleType,true),StructField("component_661",DoubleType,true),StructField("component_662",DoubleType,true),StructField("component_663",DoubleType,true),StructField("component_664",DoubleType,true),StructField("component_665",DoubleType,true),StructField("component_666",DoubleType,true),StructField("component_667",DoubleType,true),StructField("component_668",DoubleType,true),StructField("component_669",DoubleType,true),StructField("component_670",DoubleType,true),StructField("component_671",DoubleType,true),StructField("component_672",DoubleType,true),StructField("component_673",DoubleType,true),StructField("component_674",DoubleType,true),StructField("component_675",DoubleType,true),StructField("component_676",DoubleType,true),StructField("component_677",DoubleType,true),StructField("component_678",DoubleType,true),StructField("component_679",DoubleType,true),StructField("component_680",DoubleType,true),StructField("component_681",DoubleType,true),StructField("component_682",DoubleType,true),StructField("component_683",DoubleType,true),StructField("component_684",DoubleType,true),StructField("component_685",DoubleType,true),StructField("component_686",DoubleType,true),StructField("component_687",DoubleType,true),StructField("component_688",DoubleType,true),StructField("component_689",DoubleType,true),StructField("component_690",DoubleType,true),StructField("component_691",DoubleType,true),StructField("component_692",DoubleType,true),StructField("component_693",DoubleType,true),StructField("component_694",DoubleType,true),StructField("component_695",DoubleType,true),StructField("component_696",DoubleType,true),StructField("component_697",DoubleType,true),StructField("component_698",DoubleType,true),StructField("component_699",DoubleType,true),StructField("component_700",DoubleType,true),StructField("component_701",DoubleType,true),StructField("component_702",DoubleType,true),StructField("component_703",DoubleType,true),StructField("component_704",DoubleType,true),StructField("component_705",DoubleType,true),StructField("component_706",DoubleType,true),StructField("component_707",DoubleType,true),StructField("component_708",DoubleType,true),StructField("component_709",DoubleType,true),StructField("component_710",DoubleType,true),StructField("component_711",DoubleType,true),StructField("component_712",DoubleType,true),StructField("component_713",DoubleType,true),StructField("component_714",DoubleType,true),StructField("component_715",DoubleType,true),StructField("component_716",DoubleType,true),StructField("component_717",DoubleType,true),StructField("component_718",DoubleType,true),StructField("component_719",DoubleType,true),StructField("component_720",DoubleType,true),StructField("component_721",DoubleType,true),StructField("component_722",DoubleType,true),StructField("component_723",DoubleType,true),StructField("component_724",DoubleType,true),StructField("component_725",DoubleType,true),StructField("component_726",DoubleType,true),StructField("component_727",DoubleType,true),StructField("component_728",DoubleType,true),StructField("component_729",DoubleType,true),StructField("component_730",DoubleType,true),StructField("component_731",DoubleType,true),StructField("component_732",DoubleType,true),StructField("component_733",DoubleType,true),StructField("component_734",DoubleType,true),StructField("component_735",DoubleType,true),StructField("component_736",DoubleType,true),StructField("component_737",DoubleType,true),StructField("component_738",DoubleType,true),StructField("component_739",DoubleType,true),StructField("component_740",DoubleType,true),StructField("component_741",DoubleType,true),StructField("component_742",DoubleType,true),StructField("component_743",DoubleType,true),StructField("component_744",DoubleType,true),StructField("component_745",DoubleType,true),StructField("component_746",DoubleType,true),StructField("component_747",DoubleType,true),StructField("component_748",DoubleType,true),StructField("component_749",DoubleType,true),StructField("component_750",DoubleType,true),StructField("component_751",DoubleType,true),StructField("component_752",DoubleType,true),StructField("component_753",DoubleType,true),StructField("component_754",DoubleType,true),StructField("component_755",DoubleType,true),StructField("component_756",DoubleType,true),StructField("component_757",DoubleType,true),StructField("component_758",DoubleType,true),StructField("component_759",DoubleType,true),StructField("component_760",DoubleType,true),StructField("component_761",DoubleType,true),StructField("component_762",DoubleType,true),StructField("component_763",DoubleType,true),StructField("component_764",DoubleType,true),StructField("component_765",DoubleType,true),StructField("component_766",DoubleType,true),StructField("component_767",DoubleType,true),StructField("component_768",DoubleType,true),StructField("component_769",DoubleType,true),StructField("component_770",DoubleType,true),StructField("component_771",DoubleType,true),StructField("component_772",DoubleType,true),StructField("component_773",DoubleType,true),StructField("component_774",DoubleType,true),StructField("component_775",DoubleType,true),StructField("component_776",DoubleType,true),StructField("component_777",DoubleType,true),StructField("component_778",DoubleType,true),StructField("component_779",DoubleType,true),StructField("component_780",DoubleType,true),StructField("component_781",DoubleType,true),StructField("component_782",DoubleType,true),StructField("component_783",DoubleType,true),StructField("component_784",DoubleType,true),StructField("component_785",DoubleType,true),StructField("component_786",DoubleType,true),StructField("component_787",DoubleType,true),StructField("component_788",DoubleType,true),StructField("component_789",DoubleType,true),StructField("component_790",DoubleType,true),StructField("component_791",DoubleType,true),StructField("component_792",DoubleType,true),StructField("component_793",DoubleType,true),StructField("component_794",DoubleType,true),StructField("component_795",DoubleType,true),StructField("component_796",DoubleType,true),StructField("component_797",DoubleType,true),StructField("component_798",DoubleType,true),StructField("component_799",DoubleType,true),StructField("component_800",DoubleType,true),StructField("component_801",DoubleType,true),StructField("component_802",DoubleType,true),StructField("component_803",DoubleType,true),StructField("component_804",DoubleType,true),StructField("component_805",DoubleType,true),StructField("component_806",DoubleType,true),StructField("component_807",DoubleType,true),StructField("component_808",DoubleType,true),StructField("component_809",DoubleType,true),StructField("component_810",DoubleType,true),StructField("component_811",DoubleType,true),StructField("component_812",DoubleType,true),StructField("component_813",DoubleType,true),StructField("component_814",DoubleType,true),StructField("component_815",DoubleType,true),StructField("component_816",DoubleType,true),StructField("component_817",DoubleType,true),StructField("component_818",DoubleType,true),StructField("component_819",DoubleType,true),StructField("component_820",DoubleType,true),StructField("component_821",DoubleType,true),StructField("component_822",DoubleType,true),StructField("component_823",DoubleType,true),StructField("component_824",DoubleType,true),StructField("component_825",DoubleType,true),StructField("component_826",DoubleType,true),StructField("component_827",DoubleType,true),StructField("component_828",DoubleType,true),StructField("component_829",DoubleType,true),StructField("component_830",DoubleType,true),StructField("component_831",DoubleType,true),StructField("component_832",DoubleType,true),StructField("component_833",DoubleType,true),StructField("component_834",DoubleType,true),StructField("component_835",DoubleType,true),StructField("component_836",DoubleType,true),StructField("component_837",DoubleType,true),StructField("component_838",DoubleType,true),StructField("component_839",DoubleType,true),StructField("component_840",DoubleType,true),StructField("component_841",DoubleType,true),StructField("component_842",DoubleType,true),StructField("component_843",DoubleType,true),StructField("component_844",DoubleType,true),StructField("component_845",DoubleType,true),StructField("component_846",DoubleType,true),StructField("component_847",DoubleType,true),StructField("component_848",DoubleType,true),StructField("component_849",DoubleType,true),StructField("component_850",DoubleType,true),StructField("component_851",DoubleType,true),StructField("component_852",DoubleType,true),StructField("component_853",DoubleType,true),StructField("component_854",DoubleType,true),StructField("component_855",DoubleType,true),StructField("component_856",DoubleType,true),StructField("component_857",DoubleType,true),StructField("component_858",DoubleType,true),StructField("component_859",DoubleType,true),StructField("component_860",DoubleType,true),StructField("component_861",DoubleType,true),StructField("component_862",DoubleType,true),StructField("component_863",DoubleType,true),StructField("component_864",DoubleType,true),StructField("component_865",DoubleType,true),StructField("component_866",DoubleType,true),StructField("component_867",DoubleType,true),StructField("component_868",DoubleType,true),StructField("component_869",DoubleType,true),StructField("component_870",DoubleType,true),StructField("component_871",DoubleType,true),StructField("component_872",DoubleType,true),StructField("component_873",DoubleType,true),StructField("component_874",DoubleType,true),StructField("component_875",DoubleType,true),StructField("component_876",DoubleType,true),StructField("component_877",DoubleType,true),StructField("component_878",DoubleType,true),StructField("component_879",DoubleType,true),StructField("component_880",DoubleType,true),StructField("component_881",DoubleType,true),StructField("component_882",DoubleType,true),StructField("component_883",DoubleType,true),StructField("component_884",DoubleType,true),StructField("component_885",DoubleType,true),StructField("component_886",DoubleType,true),StructField("component_887",DoubleType,true),StructField("component_888",DoubleType,true),StructField("component_889",DoubleType,true),StructField("component_890",DoubleType,true),StructField("component_891",DoubleType,true),StructField("component_892",DoubleType,true),StructField("component_893",DoubleType,true),StructField("component_894",DoubleType,true),StructField("component_895",DoubleType,true),StructField("component_896",DoubleType,true),StructField("component_897",DoubleType,true),StructField("component_898",DoubleType,true),StructField("component_899",DoubleType,true),StructField("component_900",DoubleType,true),StructField("component_901",DoubleType,true),StructField("component_902",DoubleType,true),StructField("component_903",DoubleType,true),StructField("component_904",DoubleType,true),StructField("component_905",DoubleType,true),StructField("component_906",DoubleType,true),StructField("component_907",DoubleType,true),StructField("component_908",DoubleType,true),StructField("component_909",DoubleType,true),StructField("component_910",DoubleType,true),StructField("component_911",DoubleType,true),StructField("component_912",DoubleType,true),StructField("component_913",DoubleType,true),StructField("component_914",DoubleType,true),StructField("component_915",DoubleType,true),StructField("component_916",DoubleType,true),StructField("component_917",DoubleType,true),StructField("component_918",DoubleType,true),StructField("component_919",DoubleType,true),StructField("component_920",DoubleType,true),StructField("component_921",DoubleType,true),StructField("component_922",DoubleType,true),StructField("component_923",DoubleType,true),StructField("component_924",DoubleType,true),StructField("component_925",DoubleType,true),StructField("component_926",DoubleType,true),StructField("component_927",DoubleType,true),StructField("component_928",DoubleType,true),StructField("component_929",DoubleType,true),StructField("component_930",DoubleType,true),StructField("component_931",DoubleType,true),StructField("component_932",DoubleType,true),StructField("component_933",DoubleType,true),StructField("component_934",DoubleType,true),StructField("component_935",DoubleType,true),StructField("component_936",DoubleType,true),StructField("component_937",DoubleType,true),StructField("component_938",DoubleType,true),StructField("component_939",DoubleType,true),StructField("component_940",DoubleType,true),StructField("component_941",DoubleType,true),StructField("component_942",DoubleType,true),StructField("component_943",DoubleType,true),StructField("component_944",DoubleType,true),StructField("component_945",DoubleType,true),StructField("component_946",DoubleType,true),StructField("component_947",DoubleType,true),StructField("component_948",DoubleType,true),StructField("component_949",DoubleType,true),StructField("component_950",DoubleType,true),StructField("component_951",DoubleType,true),StructField("component_952",DoubleType,true),StructField("component_953",DoubleType,true),StructField("component_954",DoubleType,true),StructField("component_955",DoubleType,true),StructField("component_956",DoubleType,true),StructField("component_957",DoubleType,true),StructField("component_958",DoubleType,true),StructField("component_959",DoubleType,true),StructField("component_960",DoubleType,true),StructField("component_961",DoubleType,true),StructField("component_962",DoubleType,true),StructField("component_963",DoubleType,true),StructField("component_964",DoubleType,true),StructField("component_965",DoubleType,true),StructField("component_966",DoubleType,true),StructField("component_967",DoubleType,true),StructField("component_968",DoubleType,true),StructField("component_969",DoubleType,true),StructField("component_970",DoubleType,true),StructField("component_971",DoubleType,true),StructField("component_972",DoubleType,true),StructField("component_973",DoubleType,true),StructField("component_974",DoubleType,true),StructField("component_975",DoubleType,true),StructField("component_976",DoubleType,true),StructField("component_977",DoubleType,true),StructField("component_978",DoubleType,true),StructField("component_979",DoubleType,true),StructField("component_980",DoubleType,true),StructField("component_981",DoubleType,true),StructField("component_982",DoubleType,true),StructField("component_983",DoubleType,true),StructField("component_984",DoubleType,true),StructField("component_985",DoubleType,true),StructField("component_986",DoubleType,true),StructField("component_987",DoubleType,true),StructField("component_988",DoubleType,true),StructField("component_989",DoubleType,true),StructField("component_990",DoubleType,true),StructField("component_991",DoubleType,true),StructField("component_992",DoubleType,true),StructField("component_993",DoubleType,true),StructField("component_994",DoubleType,true),StructField("component_995",DoubleType,true),StructField("component_996",DoubleType,true),StructField("component_997",DoubleType,true),StructField("component_998",DoubleType,true),StructField("component_999",DoubleType,true),StructField("component_1000",DoubleType,true),StructField("component_1001",DoubleType,true),StructField("component_1002",DoubleType,true),StructField("component_1003",DoubleType,true),StructField("component_1004",DoubleType,true),StructField("component_1005",DoubleType,true),StructField("component_1006",DoubleType,true),StructField("component_1007",DoubleType,true),StructField("component_1008",DoubleType,true),StructField("component_1009",DoubleType,true),StructField("component_1010",DoubleType,true),StructField("component_1011",DoubleType,true),StructField("component_1012",DoubleType,true),StructField("component_1013",DoubleType,true),StructField("component_1014",DoubleType,true),StructField("component_1015",DoubleType,true),StructField("component_1016",DoubleType,true),StructField("component_1017",DoubleType,true),StructField("component_1018",DoubleType,true),StructField("component_1019",DoubleType,true),StructField("component_1020",DoubleType,true),StructField("component_1021",DoubleType,true),StructField("component_1022",DoubleType,true),StructField("component_1023",DoubleType,true),StructField("component_1024",DoubleType,true),StructField("component_1025",DoubleType,true),StructField("component_1026",DoubleType,true),StructField("component_1027",DoubleType,true),StructField("component_1028",DoubleType,true),StructField("component_1029",DoubleType,true),StructField("component_1030",DoubleType,true),StructField("component_1031",DoubleType,true),StructField("component_1032",DoubleType,true),StructField("component_1033",DoubleType,true),StructField("component_1034",DoubleType,true),StructField("component_1035",DoubleType,true),StructField("component_1036",DoubleType,true),StructField("component_1037",DoubleType,true),StructField("component_1038",DoubleType,true),StructField("component_1039",DoubleType,true),StructField("component_1040",DoubleType,true),StructField("component_1041",DoubleType,true),StructField("component_1042",DoubleType,true),StructField("component_1043",DoubleType,true),StructField("component_1044",DoubleType,true),StructField("component_1045",DoubleType,true),StructField("component_1046",DoubleType,true),StructField("component_1047",DoubleType,true),StructField("component_1048",DoubleType,true),StructField("component_1049",DoubleType,true),StructField("component_1050",DoubleType,true),StructField("component_1051",DoubleType,true),StructField("component_1052",DoubleType,true),StructField("component_1053",DoubleType,true),StructField("component_1054",DoubleType,true),StructField("component_1055",DoubleType,true),StructField("component_1056",DoubleType,true),StructField("component_1057",DoubleType,true),StructField("component_1058",DoubleType,true),StructField("component_1059",DoubleType,true),StructField("component_1060",DoubleType,true),StructField("component_1061",DoubleType,true),StructField("component_1062",DoubleType,true),StructField("component_1063",DoubleType,true),StructField("component_1064",DoubleType,true),StructField("component_1065",DoubleType,true),StructField("component_1066",DoubleType,true),StructField("component_1067",DoubleType,true),StructField("component_1068",DoubleType,true),StructField("component_1069",DoubleType,true),StructField("component_1070",DoubleType,true),StructField("component_1071",DoubleType,true),StructField("component_1072",DoubleType,true),StructField("component_1073",DoubleType,true),StructField("component_1074",DoubleType,true),StructField("component_1075",DoubleType,true),StructField("component_1076",DoubleType,true),StructField("component_1077",DoubleType,true),StructField("component_1078",DoubleType,true),StructField("component_1079",DoubleType,true),StructField("component_1080",DoubleType,true),StructField("component_1081",DoubleType,true),StructField("component_1082",DoubleType,true),StructField("component_1083",DoubleType,true),StructField("component_1084",DoubleType,true),StructField("component_1085",DoubleType,true),StructField("component_1086",DoubleType,true),StructField("component_1087",DoubleType,true),StructField("component_1088",DoubleType,true),StructField("component_1089",DoubleType,true),StructField("component_1090",DoubleType,true),StructField("component_1091",DoubleType,true),StructField("component_1092",DoubleType,true),StructField("component_1093",DoubleType,true),StructField("component_1094",DoubleType,true),StructField("component_1095",DoubleType,true),StructField("component_1096",DoubleType,true),StructField("component_1097",DoubleType,true),StructField("component_1098",DoubleType,true),StructField("component_1099",DoubleType,true),StructField("component_1100",DoubleType,true),StructField("component_1101",DoubleType,true),StructField("component_1102",DoubleType,true),StructField("component_1103",DoubleType,true),StructField("component_1104",DoubleType,true),StructField("component_1105",DoubleType,true),StructField("component_1106",DoubleType,true),StructField("component_1107",DoubleType,true),StructField("component_1108",DoubleType,true),StructField("component_1109",DoubleType,true),StructField("component_1110",DoubleType,true),StructField("component_1111",DoubleType,true),StructField("component_1112",DoubleType,true),StructField("component_1113",DoubleType,true),StructField("component_1114",DoubleType,true),StructField("component_1115",DoubleType,true),StructField("component_1116",DoubleType,true),StructField("component_1117",DoubleType,true),StructField("component_1118",DoubleType,true),StructField("component_1119",DoubleType,true),StructField("component_1120",DoubleType,true),StructField("component_1121",DoubleType,true),StructField("component_1122",DoubleType,true),StructField("component_1123",DoubleType,true),StructField("component_1124",DoubleType,true),StructField("component_1125",DoubleType,true),StructField("component_1126",DoubleType,true),StructField("component_1127",DoubleType,true),StructField("component_1128",DoubleType,true),StructField("component_1129",DoubleType,true),StructField("component_1130",DoubleType,true),StructField("component_1131",DoubleType,true),StructField("component_1132",DoubleType,true),StructField("component_1133",DoubleType,true),StructField("component_1134",DoubleType,true),StructField("component_1135",DoubleType,true),StructField("component_1136",DoubleType,true),StructField("component_1137",DoubleType,true),StructField("component_1138",DoubleType,true),StructField("component_1139",DoubleType,true),StructField("component_1140",DoubleType,true),StructField("component_1141",DoubleType,true),StructField("component_1142",DoubleType,true),StructField("component_1143",DoubleType,true),StructField("component_1144",DoubleType,true),StructField("component_1145",DoubleType,true),StructField("component_1146",DoubleType,true),StructField("component_1147",DoubleType,true),StructField("component_1148",DoubleType,true),StructField("component_1149",DoubleType,true),StructField("component_1150",DoubleType,true),StructField("component_1151",DoubleType,true),StructField("component_1152",DoubleType,true),StructField("component_1153",DoubleType,true),StructField("component_1154",DoubleType,true),StructField("component_1155",DoubleType,true),StructField("component_1156",DoubleType,true),StructField("component_1157",DoubleType,true),StructField("component_1158",DoubleType,true),StructField("component_1159",DoubleType,true),StructField("component_1160",DoubleType,true),StructField("component_1161",DoubleType,true),StructField("component_1162",DoubleType,true),StructField("component_1163",DoubleType,true),StructField("component_1164",DoubleType,true),StructField("component_1165",DoubleType,true),StructField("component_1166",DoubleType,true),StructField("component_1167",DoubleType,true),StructField("component_1168",DoubleType,true),StructField("component_1169",DoubleType,true),StructField("component_1170",DoubleType,true),StructField("component_1171",DoubleType,true),StructField("component_1172",DoubleType,true),StructField("component_1173",DoubleType,true),StructField("component_1174",DoubleType,true),StructField("component_1175",DoubleType,true),StructField("component_1176",DoubleType,true),StructField(instanceName,StringType,true)))




# --------------------------------------------------------------------------------------------------------

#AUDIO SIMILARITY

#Question 1a)

audio_dataset_name = "msd-jmir-spectral-all-all-v1.0"
schema = audio_dataset_schemas[audio_dataset_name]
data = spark.read.format("com.databricks.spark.csv") \
  .option("header", "true") \
  .option("inferSchema", "false") \
  .schema(schema) \
  .load("hdfs:///data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv")

data.show()
# +----------------------------------------------+---------------------------------------------------+------------------------------------------+----------------------------------------+-------------------------------------------------+---------------------------------------------+-----------------------------------------------------------+-------------------------------------------+-----------------------------------+----------------------------------------+-------------------------------+-----------------------------+--------------------------------------+----------------------------------+------------------------------------------------+--------------------------------+--------------------+
# |Spectral_Centroid_Overall_Standard_Deviation_1|Spectral_Rolloff_Point_Overall_Standard_Deviation_1|Spectral_Flux_Overall_Standard_Deviation_1|Compactness_Overall_Standard_Deviation_1|Spectral_Variability_Overall_Standard_Deviation_1|Root_Mean_Square_Overall_Standard_Deviation_1|Fraction_Of_Low_Energy_Windows_Overall_Standard_Deviation_1|Zero_Crossings_Overall_Standard_Deviation_1|Spectral_Centroid_Overall_Average_1|Spectral_Rolloff_Point_Overall_Average_1|Spectral_Flux_Overall_Average_1|Compactness_Overall_Average_1|Spectral_Variability_Overall_Average_1|Root_Mean_Square_Overall_Average_1|Fraction_Of_Low_Energy_Windows_Overall_Average_1|Zero_Crossings_Overall_Average_1|         MSD_TRACKID|
# +----------------------------------------------+---------------------------------------------------+------------------------------------------+----------------------------------------+-------------------------------------------------+---------------------------------------------+-----------------------------------------------------------+-------------------------------------------+-----------------------------------+----------------------------------------+-------------------------------+-----------------------------+--------------------------------------+----------------------------------+------------------------------------------------+--------------------------------+--------------------+
# |                                         8.501|                                            0.07007|                                  0.005855|                                   200.6|                                         0.003042|                                      0.09163|                                                    0.05096|                                      21.18|                              7.432|                                 0.05245|                       0.003384|                       1570.0|                              0.004289|                            0.1532|                                          0.5988|                           25.07|'TRHFHYX12903CAF953'|
# |                                         5.101|                                            0.04946|                                  0.007952|                                   241.3|                                         0.002879|                                      0.08716|                                                    0.03366|                                      13.13|                              9.995|                                 0.07575|                        0.01031|                       1455.0|                              0.008896|                            0.3404|                                          0.5227|                           34.82|'TRHFHAU128F9341A0E'|
# |                                         8.101|                                            0.06402|                                  0.002458|                                   238.5|                                         0.002335|                                      0.08902|                                                    0.06764|                                      18.71|                              15.35|                                   0.102|                       0.001901|                       1712.0|                              0.004152|                            0.1649|                                          0.5467|                           41.47|'TRHFHLP128F14947A7'|
# |                                         7.226|                                            0.05985|                                  0.005215|                                   194.7|                                         0.002057|                                      0.05784|                                                    0.04056|                                      15.88|                              12.98|                                  0.1094|                       0.008331|                       1595.0|                              0.008042|                            0.3087|                                          0.5067|                           39.75|'TRHFHFF128F930AC11'|
# |                                         4.304|                                            0.03282|                                  0.001262|                                   279.3|                                         0.002383|                                      0.08844|                                                    0.07417|                                      10.88|                              7.721|                                 0.04463|                       0.001093|                       1778.0|                              0.004259|                            0.1622|                                          0.5364|                           18.54|'TRHFHYJ128F4234782'|
# |                                         2.724|                                            0.02075|                                  0.001779|                                   203.1|                                         0.001305|                                       0.0453|                                                    0.05082|                                      9.718|                              5.263|                                 0.02806|                        8.93E-4|                       1748.0|                              0.002865|                            0.1078|                                          0.5391|                            19.3|'TRHFHHR128F9339010'|
# |                                         15.66|                                            0.09097|                                  5.162E-4|                                   178.1|                                         0.001069|                                      0.03922|                                                    0.08063|                                      33.94|                              9.158|                                 0.05251|                       2.946E-4|                       1621.0|                              0.001542|                           0.05869|                                          0.5568|                           23.59|'TRHFHMB128F4213BC9'|
# |                                         2.161|                                            0.01658|                                  0.003491|                                   239.0|                                           0.0018|                                      0.05721|                                                    0.04387|                                      7.207|                              3.613|                                 0.02298|                       0.002387|                       1676.0|                              0.004306|                            0.1585|                                          0.5359|                           10.82|'TRHFHWT128F429032D'|
# |                                         8.862|                                            0.07809|                                  0.005187|                                   218.2|                                         0.003705|                                       0.1118|                                                    0.05035|                                      23.79|                              7.212|                                 0.05154|                       0.003541|                       1547.0|                              0.006084|                             0.212|                                           0.554|                           24.34|'TRHFHKO12903CBAF09'|
# |                                         17.28|                                             0.1152|                                   0.01479|                                   233.2|                                         0.004955|                                       0.1585|                                                    0.04368|                                      36.05|                              13.94|                                 0.09003|                       0.009435|                       1599.0|                              0.007048|                            0.2618|                                          0.5499|                           37.91|'TRHFHOB128F425F027'|
# |                                         23.56|                                             0.1236|                                  8.302E-4|                                   382.7|                                         0.001693|                                      0.06474|                                                    0.06028|                                      43.89|                              17.67|                                 0.09993|                       3.809E-4|                       1588.0|                              0.001678|                            0.0661|                                          0.6046|                           39.81|'TRHFHTT128E0789A6E'|
# |                                         6.563|                                            0.05805|                                  0.007819|                                   235.6|                                          0.00288|                                      0.09405|                                                    0.04608|                                       16.5|                                7.8|                                  0.0557|                       0.005552|                       1602.0|                              0.006178|                            0.2343|                                          0.5989|                           24.77|'TRHFHQQ128EF3601ED'|
# |                                         4.857|                                            0.04047|                                  0.002639|                                   260.9|                                         0.002403|                                      0.08423|                                                    0.04351|                                       12.8|                              6.859|                                 0.04453|                       0.002576|                       1673.0|                              0.005875|                            0.2233|                                          0.4709|                           19.75|'TRHFHOX128F92E6483'|
# |                                         4.156|                                            0.03643|                                  0.004138|                                   192.7|                                         0.002181|                                      0.07433|                                                    0.04693|                                      10.98|                              9.267|                                 0.05836|                       0.004158|                       1645.0|                              0.006144|                            0.2401|                                           0.534|                           28.19|'TRHFHSJ128F92EEFFD'|
# |                                         3.894|                                            0.03426|                                  0.001304|                                   184.1|                                         9.777E-4|                                      0.02615|                                                    0.03139|                                      9.208|                              13.26|                                  0.1097|                       0.002733|                       1567.0|                              0.005805|                            0.2341|                                          0.5061|                           41.04|'TRHFHVK128F425EA92'|
# |                                          6.38|                                            0.03926|                                  0.002193|                                   208.6|                                         0.001873|                                       0.0692|                                                    0.07661|                                      13.76|                               13.2|                                 0.07907|                       0.001294|                       1574.0|                              0.003035|                            0.1195|                                          0.6041|                            33.5|'TRHFHRG128F931A920'|
# |                                         4.729|                                            0.04717|                                  0.003249|                                   175.1|                                         0.002691|                                      0.09461|                                                    0.05813|                                      14.74|                              5.412|                                 0.03377|                        0.00293|                       1653.0|                              0.005649|                            0.2088|                                           0.494|                           16.46|'TRHFHZI128F42ADA12'|
# |                                         7.474|                                            0.06978|                                  0.007286|                                   198.5|                                         0.002855|                                      0.09813|                                                     0.0498|                                      18.75|                              12.57|                                 0.09177|                       0.006001|                       1551.0|                              0.006366|                             0.249|                                          0.5607|                            38.2|'TRHFHIC128F149297B'|
# |                                         3.787|                                            0.02626|                                  5.253E-4|                                   301.4|                                          0.00105|                                      0.03871|                                                    0.07954|                                      9.597|                              7.893|                                 0.04765|                       3.155E-4|                       1853.0|                              0.002129|                           0.08137|                                          0.5251|                           20.57|'TRHFHQX128F934415F'|
# |                                         5.876|                                             0.0489|                                  2.098E-4|                                   226.2|                                         7.899E-4|                                      0.02879|                                                     0.0649|                                      15.87|                              8.452|                                 0.05066|                       1.015E-4|                       1797.0|                              0.001363|                           0.05155|                                          0.5631|                           22.79|'TRHFHFB12903CD926D'|
# +----------------------------------------------+---------------------------------------------------+------------------------------------------+----------------------------------------+-------------------------------------------------+---------------------------------------------+-----------------------------------------------------------+-------------------------------------------+-----------------------------------+----------------------------------------+-------------------------------+-----------------------------+--------------------------------------+----------------------------------+------------------------------------------------+--------------------------------+--------------------+
# only showing top 20 rows

data.describe().show()
# +-------+----------------------------------------------+---------------------------------------------------+------------------------------------------+----------------------------------------+-------------------------------------------------+---------------------------------------------+-----------------------------------------------------------+-------------------------------------------+-----------------------------------+----------------------------------------+-------------------------------+-----------------------------+--------------------------------------+----------------------------------+------------------------------------------------+--------------------------------+--------------------+
# |summary|Spectral_Centroid_Overall_Standard_Deviation_1|Spectral_Rolloff_Point_Overall_Standard_Deviation_1|Spectral_Flux_Overall_Standard_Deviation_1|Compactness_Overall_Standard_Deviation_1|Spectral_Variability_Overall_Standard_Deviation_1|Root_Mean_Square_Overall_Standard_Deviation_1|Fraction_Of_Low_Energy_Windows_Overall_Standard_Deviation_1|Zero_Crossings_Overall_Standard_Deviation_1|Spectral_Centroid_Overall_Average_1|Spectral_Rolloff_Point_Overall_Average_1|Spectral_Flux_Overall_Average_1|Compactness_Overall_Average_1|Spectral_Variability_Overall_Average_1|Root_Mean_Square_Overall_Average_1|Fraction_Of_Low_Energy_Windows_Overall_Average_1|Zero_Crossings_Overall_Average_1|         MSD_TRACKID|
# +-------+----------------------------------------------+---------------------------------------------------+------------------------------------------+----------------------------------------+-------------------------------------------------+---------------------------------------------+-----------------------------------------------------------+-------------------------------------------+-----------------------------------+----------------------------------------+-------------------------------+-----------------------------+--------------------------------------+----------------------------------+------------------------------------------------+--------------------------------+--------------------+
# |  count|                                        994615|                                             994615|                                    994615|                                  994615|                                           994615|                                       994615|                                                     994615|                                     994615|                             994615|                                  994615|                         994615|                       994615|                                994615|                            994615|                                          994615|                          994615|              994615|
# |   mean|                             6.945075321958795|                                0.05570656330077427|                      0.003945429422315233|                      222.51785769557006|                             0.002227141353483172|                          0.07420121987116508|                                       0.060207318112032934|                         16.802847226233283|                  9.110257231921974|                    0.061943205894792734|           0.002932155180292255|           1638.7321744558617|                  0.004395476870406151|               0.16592784214140105|                              0.5562831490668246|              26.680410639768105|                null|
# | stddev|                             3.631804093939155|                               0.026500210060303366|                      0.003265333343455...|                       59.72639079514588|                             0.001039740410980...|                          0.03176619445967484|                                        0.01851647926605091|                          7.530133216657504|                 3.8436388429686703|                    0.029016729824972554|           0.002491152580972...|           106.10634441394468|                  0.001995891823991...|               0.07429866377222298|                             0.04755380098948043|              10.394734197335119|                null|
# |    min|                                           0.0|                                                0.0|                                       0.0|                                     0.0|                                              0.0|                                          0.0|                                                        0.0|                                        0.0|                                0.0|                                     0.0|                            0.0|                          0.0|                                   0.0|                               0.0|                                             0.0|                             0.0|'TRAAAAK128F9318786'|
# |    max|                                         73.31|                                             0.3739|                                   0.07164|                                 10290.0|                                          0.01256|                                       0.3676|                                                     0.4938|                                      141.6|                              133.0|                                  0.7367|                        0.07549|                      24760.0|                               0.02366|                            0.8564|                                          0.9538|                           280.5|'TRZZZZO128F428E2D4'|
# +-------+----------------------------------------------+---------------------------------------------------+------------------------------------------+----------------------------------------+-------------------------------------------------+---------------------------------------------+-----------------------------------------------------------+-------------------------------------------+-----------------------------------+----------------------------------------+-------------------------------+-----------------------------+--------------------------------------+----------------------------------+------------------------------------------------+--------------------------------+--------------------+



from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler, StringIndexer
import pandas as pd

assembler = VectorAssembler(
  inputCols=data.columns[:-1],
  outputCol="Features"
)

features = assembler.transform(data).select(["Features", "MSD_TRACKID"])
features.cache()
features.count()

features.show(10, 80)
# +--------------------------------------------------------------------------------+--------------------+
# |                                                                        Features|         MSD_TRACKID|
# +--------------------------------------------------------------------------------+--------------------+
# |[8.501,0.07007,0.005855,200.6,0.003042,0.09163,0.05096,21.18,7.432,0.05245,0....|'TRHFHYX12903CAF953'|
# |[5.101,0.04946,0.007952,241.3,0.002879,0.08716,0.03366,13.13,9.995,0.07575,0....|'TRHFHAU128F9341A0E'|
# |[8.101,0.06402,0.002458,238.5,0.002335,0.08902,0.06764,18.71,15.35,0.102,0.00...|'TRHFHLP128F14947A7'|
# |[7.226,0.05985,0.005215,194.7,0.002057,0.05784,0.04056,15.88,12.98,0.1094,0.0...|'TRHFHFF128F930AC11'|
# |[4.304,0.03282,0.001262,279.3,0.002383,0.08844,0.07417,10.88,7.721,0.04463,0....|'TRHFHYJ128F4234782'|
# |[2.724,0.02075,0.001779,203.1,0.001305,0.0453,0.05082,9.718,5.263,0.02806,8.9...|'TRHFHHR128F9339010'|
# |[15.66,0.09097,5.162E-4,178.1,0.001069,0.03922,0.08063,33.94,9.158,0.05251,2....|'TRHFHMB128F4213BC9'|
# |[2.161,0.01658,0.003491,239.0,0.0018,0.05721,0.04387,7.207,3.613,0.02298,0.00...|'TRHFHWT128F429032D'|
# |[8.862,0.07809,0.005187,218.2,0.003705,0.1118,0.05035,23.79,7.212,0.05154,0.0...|'TRHFHKO12903CBAF09'|
# |[17.28,0.1152,0.01479,233.2,0.004955,0.1585,0.04368,36.05,13.94,0.09003,0.009...|'TRHFHOB128F425F027'|
# +--------------------------------------------------------------------------------+--------------------+
# only showing top 10 rows


correlations = Correlation.corr(features, 'Features', 'pearson').collect()[0][0].toArray()

correlations

# array([[1., 0.95460984, 0.26350749, 0.03911548, 0.34039826,
#               0.36412222, 0.11023803, 0.97155987, 0.62371, 0.59024565,
#               0.14098191, -0.14027388, 0.05139935, 0.05053354, 0.15038239,
#               0.62020385],
#              [0.95460984, 1., 0.30835248, -0.01946768, 0.36310437,
#               0.38117284, 0.03354423, 0.97134878, 0.59390653, 0.59177998,
#               0.20299339, -0.16797304, 0.1236861, 0.1259576, 0.1702388,
#               0.65261559],
#              [0.26350749, 0.30835248, 1., -0.014022, 0.90005733,
#               0.86682291, -0.16510604, 0.30877068, 0.02690334, 0.07110143,
#               0.89180551, -0.25553163, 0.78960162, 0.76673078, 0.21404184,
#               0.14142542],
#              [0.03911548, -0.01946768, -0.014022, 1., 0.0458597,
#               0.05838977, 0.3096897, 0.00827878, -0.07593258, -0.10783167,
#               -0.06649495, 0.30508169, -0.07267878, -0.0859689, -0.06281674,
#               -0.15375305],
#              [0.34039826, 0.36310437, 0.90005733, 0.0458597, 1.,
#               0.98466128, -0.06753376, 0.3892821, -0.0135602, 0.00813641,
#               0.77460861, -0.21042888, 0.75733962, 0.72222467, 0.14808266,
#               0.07300852],
#              [0.36412222, 0.38117284, 0.86682291, 0.05838977, 0.98466128,
#               1., -0.02127568, 0.40862777, 0.030984, 0.04299419,
#               0.72967682, -0.19045653, 0.72149452, 0.69501312, 0.15032301,
#               0.10264152],
#              [0.11023803, 0.03354423, -0.16510604, 0.3096897, -0.06753376,
#               -0.02127568, 1., 0.08127188, -0.01058832, -0.06986352,
#               -0.29248656, 0.08261359, -0.30074744, -0.30787651, -0.00196908,
#               -0.11855529],
#              [0.97155987, 0.97134878, 0.30877068, 0.00827878, 0.3892821,
#               0.40862777, 0.08127188, 1., 0.54149364, 0.51631603,
#               0.17701509, -0.14703691, 0.09999793, 0.09483142, 0.16412483,
#               0.58304271],
#              [0.62371, 0.59390653, 0.02690334, -0.07593258, -0.0135602,
#               0.030984, -0.01058832, 0.54149364, 1., 0.97823934,
#               0.06163498, -0.04821076, -0.00330583, 0.04489768, 0.12173852,
#               0.95011993],
#              [0.59024565, 0.59177998, 0.07110143, -0.10783167, 0.00813641,
#               0.04299419, -0.06986352, 0.51631603, 0.97823934, 1.,
#               0.126256, -0.09289287, 0.06151628, 0.11160835, 0.14283092,
#               0.96518441],
#              [0.14098191, 0.20299339, 0.89180551, -0.06649495, 0.77460861,
#               0.72967682, -0.29248656, 0.17701509, 0.06163498, 0.126256,
#               1., -0.29138423, 0.91796032, 0.91118876, 0.02886198,
#               0.19388445],
#              [-0.14027388, -0.16797304, -0.25553163, 0.30508169, -0.21042888,
#               -0.19045653, 0.08261359, -0.14703691, -0.04821076, -0.09289287,
#               -0.29138423, 1., -0.21616705, -0.21744046, 0.11054571,
#               -0.11178118],
#              [0.05139935, 0.1236861, 0.78960162, -0.07267878, 0.75733962,
#               0.72149452, -0.30074744, 0.09999793, -0.00330583, 0.06151628,
#               0.91796032, -0.21616705, 1., 0.99567025, -0.08388292,
#               0.13273657],
#              [0.05053354, 0.1259576, 0.76673078, -0.0859689, 0.72222467,
#               0.69501312, -0.30787651, 0.09483142, 0.04489768, 0.11160835,
#               0.91118876, -0.21744046, 0.99567025, 1., -0.08442036,
#               0.18093771],
#              [0.15038239, 0.1702388, 0.21404184, -0.06281674, 0.14808266,
#               0.15032301, -0.00196908, 0.16412483, 0.12173852, 0.14283092,
#               0.02886198, 0.11054571, -0.08388292, -0.08442036, 1.,
#               0.16734833],
#              [0.62020385, 0.65261559, 0.14142542, -0.15375305, 0.07300852,
#               0.10264152, -0.11855529, 0.58304271, 0.95011993, 0.96518441,
#               0.19388445, -0.11178118, 0.13273657, 0.18093771, 0.16734833,
#               1.]])


for i in range(0, correlations.shape[0]):
    for j in range(i + 1, correlations.shape[1]):
        if correlations[i, j] > 0.9:
            print((i, j))

# (0, 1)
# (0, 7)
# (1, 7)
# (2, 4)
# (4, 5)
# (8, 9)
# (8, 15)
# (9, 15)
# (10, 12)
# (10, 13)
# (12, 13)

#highly correlated variables to delete
#0,4,7,9,12,13,15

data_updated = data

data_updated = data_updated.drop(data_updated.columns[15])
data_updated = data_updated.drop(data_updated.columns[13])
data_updated = data_updated.drop(data_updated.columns[12])
data_updated = data_updated.drop(data_updated.columns[9])
data_updated = data_updated.drop(data_updated.columns[7])
data_updated = data_updated.drop(data_updated.columns[4])
data_updated = data_updated.drop(data_updated.columns[0])

data_updated.show()
# +---------------------------------------------------+------------------------------------------+----------------------------------------+---------------------------------------------+-----------------------------------------------------------+-----------------------------------+-------------------------------+-----------------------------+------------------------------------------------+--------------------+
# |Spectral_Rolloff_Point_Overall_Standard_Deviation_1|Spectral_Flux_Overall_Standard_Deviation_1|Compactness_Overall_Standard_Deviation_1|Root_Mean_Square_Overall_Standard_Deviation_1|Fraction_Of_Low_Energy_Windows_Overall_Standard_Deviation_1|Spectral_Centroid_Overall_Average_1|Spectral_Flux_Overall_Average_1|Compactness_Overall_Average_1|Fraction_Of_Low_Energy_Windows_Overall_Average_1|         MSD_TRACKID|
# +---------------------------------------------------+------------------------------------------+----------------------------------------+---------------------------------------------+-----------------------------------------------------------+-----------------------------------+-------------------------------+-----------------------------+------------------------------------------------+--------------------+
# |                                            0.07007|                                  0.005855|                                   200.6|                                      0.09163|                                                    0.05096|                              7.432|                       0.003384|                       1570.0|                                          0.5988|'TRHFHYX12903CAF953'|
# |                                            0.04946|                                  0.007952|                                   241.3|                                      0.08716|                                                    0.03366|                              9.995|                        0.01031|                       1455.0|                                          0.5227|'TRHFHAU128F9341A0E'|
# |                                            0.06402|                                  0.002458|                                   238.5|                                      0.08902|                                                    0.06764|                              15.35|                       0.001901|                       1712.0|                                          0.5467|'TRHFHLP128F14947A7'|
# |                                            0.05985|                                  0.005215|                                   194.7|                                      0.05784|                                                    0.04056|                              12.98|                       0.008331|                       1595.0|                                          0.5067|'TRHFHFF128F930AC11'|
# |                                            0.03282|                                  0.001262|                                   279.3|                                      0.08844|                                                    0.07417|                              7.721|                       0.001093|                       1778.0|                                          0.5364|'TRHFHYJ128F4234782'|
# |                                            0.02075|                                  0.001779|                                   203.1|                                       0.0453|                                                    0.05082|                              5.263|                        8.93E-4|                       1748.0|                                          0.5391|'TRHFHHR128F9339010'|
# |                                            0.09097|                                  5.162E-4|                                   178.1|                                      0.03922|                                                    0.08063|                              9.158|                       2.946E-4|                       1621.0|                                          0.5568|'TRHFHMB128F4213BC9'|
# |                                            0.01658|                                  0.003491|                                   239.0|                                      0.05721|                                                    0.04387|                              3.613|                       0.002387|                       1676.0|                                          0.5359|'TRHFHWT128F429032D'|
# |                                            0.07809|                                  0.005187|                                   218.2|                                       0.1118|                                                    0.05035|                              7.212|                       0.003541|                       1547.0|                                           0.554|'TRHFHKO12903CBAF09'|
# |                                             0.1152|                                   0.01479|                                   233.2|                                       0.1585|                                                    0.04368|                              13.94|                       0.009435|                       1599.0|                                          0.5499|'TRHFHOB128F425F027'|
# |                                             0.1236|                                  8.302E-4|                                   382.7|                                      0.06474|                                                    0.06028|                              17.67|                       3.809E-4|                       1588.0|                                          0.6046|'TRHFHTT128E0789A6E'|
# |                                            0.05805|                                  0.007819|                                   235.6|                                      0.09405|                                                    0.04608|                                7.8|                       0.005552|                       1602.0|                                          0.5989|'TRHFHQQ128EF3601ED'|
# |                                            0.04047|                                  0.002639|                                   260.9|                                      0.08423|                                                    0.04351|                              6.859|                       0.002576|                       1673.0|                                          0.4709|'TRHFHOX128F92E6483'|
# |                                            0.03643|                                  0.004138|                                   192.7|                                      0.07433|                                                    0.04693|                              9.267|                       0.004158|                       1645.0|                                           0.534|'TRHFHSJ128F92EEFFD'|
# |                                            0.03426|                                  0.001304|                                   184.1|                                      0.02615|                                                    0.03139|                              13.26|                       0.002733|                       1567.0|                                          0.5061|'TRHFHVK128F425EA92'|
# |                                            0.03926|                                  0.002193|                                   208.6|                                       0.0692|                                                    0.07661|                               13.2|                       0.001294|                       1574.0|                                          0.6041|'TRHFHRG128F931A920'|
# |                                            0.04717|                                  0.003249|                                   175.1|                                      0.09461|                                                    0.05813|                              5.412|                        0.00293|                       1653.0|                                           0.494|'TRHFHZI128F42ADA12'|
# |                                            0.06978|                                  0.007286|                                   198.5|                                      0.09813|                                                     0.0498|                              12.57|                       0.006001|                       1551.0|                                          0.5607|'TRHFHIC128F149297B'|
# |                                            0.02626|                                  5.253E-4|                                   301.4|                                      0.03871|                                                    0.07954|                              7.893|                       3.155E-4|                       1853.0|                                          0.5251|'TRHFHQX128F934415F'|
# |                                             0.0489|                                  2.098E-4|                                   226.2|                                      0.02879|                                                     0.0649|                              8.452|                       1.015E-4|                       1797.0|                                          0.5631|'TRHFHFB12903CD926D'|
# +---------------------------------------------------+------------------------------------------+----------------------------------------+---------------------------------------------+-----------------------------------------------------------+-----------------------------------+-------------------------------+-----------------------------+------------------------------------------------+--------------------+
# only showing top 20 rows

assembler_u = VectorAssembler(
  inputCols=data_updated.columns[:-1],
  outputCol="Features"
)

features_u = assembler_u.transform(data_updated).select(["Features", "MSD_TRACKID"])

correlations_u = Correlation.corr(features_u, 'Features', 'pearson').collect()[0][0].toArray()

correlations_u

# array([[ 1.        ,  0.30835248, -0.01946768,  0.38117284,  0.03354423,
#          0.59390653,  0.20299339, -0.16797304,  0.1702388 ],
#        [ 0.30835248,  1.        , -0.014022  ,  0.86682291, -0.16510604,
#          0.02690334,  0.89180551, -0.25553163,  0.21404184],
#        [-0.01946768, -0.014022  ,  1.        ,  0.05838977,  0.3096897 ,
#         -0.07593258, -0.06649495,  0.30508169, -0.06281674],
#        [ 0.38117284,  0.86682291,  0.05838977,  1.        , -0.02127568,
#          0.030984  ,  0.72967682, -0.19045653,  0.15032301],
#        [ 0.03354423, -0.16510604,  0.3096897 , -0.02127568,  1.        ,
#         -0.01058832, -0.29248656,  0.08261359, -0.00196908],
#        [ 0.59390653,  0.02690334, -0.07593258,  0.030984  , -0.01058832,
#          1.        ,  0.06163498, -0.04821076,  0.12173852],
#        [ 0.20299339,  0.89180551, -0.06649495,  0.72967682, -0.29248656,
#          0.06163498,  1.        , -0.29138423,  0.02886198],
#        [-0.16797304, -0.25553163,  0.30508169, -0.19045653,  0.08261359,
#         -0.04821076, -0.29138423,  1.        ,  0.11054571],
#        [ 0.1702388 ,  0.21404184, -0.06281674,  0.15032301, -0.00196908,
#          0.12173852,  0.02886198,  0.11054571,  1.        ]])




#Question 1b

genre_schema = StructType([
    StructField("track_id", StringType(), True),
    StructField("Genre", StringType(), True)
])

genre = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("codec", "gzip")
    .schema(genre_schema)
    .load("hdfs:///data/msd/genre/msd-MAGD-genreAssignment.tsv/")
)

matched_genre = genre.join(mismatches_not_accepted, on="track_id", how="left_anti")

matched_genre.groupBy("Genre").count().show(50)
# +--------------+------+
# |         Genre| count|
# +--------------+------+
# |        Stage |  1604|
# |         Vocal|  6076|
# |     Religious|  8754|
# |Easy_Listening|  1533|
# |          Jazz| 17673|
# |    Electronic| 40430|
# | International| 14094|
# |      Children|   468|
# |         Blues|  6776|
# |           Rap| 20606|
# |           RnB| 13874|
# |   Avant_Garde|  1000|
# |         Latin| 17475|
# |          Folk|  5777|
# |      Pop_Rock|234107|
# |       New Age|  3935|
# |     Classical|   542|
# |       Country| 11492|
# | Comedy_Spoken|  2051|
# |        Reggae|  6885|
# |       Holiday|   198|
# +--------------+------+


#visualising this in R

# library(RColorBrewer)
# par(mar=c(10,4,4,4))
# barplot(data$count,
#         names.arg=data$genre,
#         las=2,
#         ylim=c(0,250000),
#                   col = brewer.pal(5, name = "Pastel1"),
#                   xlab='Genre',
#                   ylab='song count',
#                   main='Song Count of Each Genre'
#         )



#Question 1c

features_u = features_u.withColumnRenamed("MSD_TRACKID", "track_id")
feature_genre = features_u.withColumn('track_id', F.substring('track_id', 2, 18))
feature_genre = feature_genre.join(matched_genre, on="track_id", how="inner")


feature_genre.show(20, False)
# +------------------+---------------------------------------------------------------------+-------------+
# |track_id          |Features                                                             |Genre        |
# +------------------+---------------------------------------------------------------------+-------------+
# |TRAAABD128F429CF47|[0.0534,0.001519,172.5,0.05665,0.0558,9.114,0.00109,1626.0,0.5304]   |Pop_Rock     |
# |TRAABPK128F424CFDB|[0.05421,0.004769,189.4,0.07297,0.0443,16.86,0.003815,1595.0,0.6286] |Pop_Rock     |
# |TRAACER128F4290F96|[0.05057,0.004536,190.6,0.09102,0.05125,8.921,0.004631,1583.0,0.5374]|Pop_Rock     |
# |TRAADYB128F92D7E73|[0.02008,0.002983,247.3,0.07965,0.06292,4.521,0.001982,1649.0,0.5126]|Jazz         |
# |TRAAGHM128EF35CF8E|[0.05034,0.005852,219.6,0.07639,0.06356,6.0,0.003312,1718.0,0.609]   |Electronic   |
# |TRAAGRV128F93526C0|[0.03922,0.002131,187.6,0.04777,0.04523,9.08,0.002354,1617.0,0.5403] |Pop_Rock     |
# |TRAAGTO128F1497E3C|[0.03838,0.001236,274.8,0.06716,0.08579,7.33,3.271E-4,1754.0,0.6142] |Pop_Rock     |
# |TRAAHAU128F9313A3D|[0.05708,8.682E-4,187.9,0.0332,0.06616,12.02,8.643E-4,1569.0,0.5874] |Pop_Rock     |
# |TRAAHEG128E07861C3|[0.08356,0.0088,252.6,0.131,0.05231,9.641,0.003686,1632.0,0.6321]    |Rap          |
# |TRAAHZP12903CA25F4|[0.07767,0.002841,265.3,0.07393,0.06671,18.56,9.622E-4,1636.0,0.6052]|Rap          |
# |TRAAICW128F1496C68|[0.03825,0.002125,185.2,0.05383,0.05024,6.082,0.001649,1617.0,0.5431]|International|
# |TRAAJJW12903CBDDCB|[0.09538,0.01063,235.2,0.1485,0.05204,11.62,0.005916,1611.0,0.6064]  |International|
# |TRAAKLX128F934CEE4|[0.08041,0.004415,243.7,0.08889,0.05899,7.036,0.003344,1589.0,0.5189]|Electronic   |
# |TRAAKWR128F931B29F|[0.03473,7.117E-4,162.8,0.03605,0.06901,3.919,3.599E-4,1631.0,0.5924]|Pop_Rock     |
# |TRAALQN128E07931A4|[0.07281,0.007399,195.0,0.1087,0.05083,8.273,0.005965,1592.0,0.5113] |Electronic   |
# |TRAAMFF12903CE8107|[0.02975,0.005098,234.2,0.07684,0.07589,4.381,0.003194,1681.0,0.5657]|Pop_Rock     |
# |TRAAMHG128F92ED7B2|[0.04798,0.002646,249.9,0.08219,0.05741,10.37,0.002537,1730.0,0.474] |International|
# |TRAAROH128F42604B0|[0.08735,0.003288,224.9,0.0861,0.05426,8.213,0.002075,1651.0,0.6163] |Electronic   |
# |TRAARQN128E07894DF|[0.08521,0.005002,487.7,0.1046,0.09092,14.01,0.004723,1434.0,0.5215] |Pop_Rock     |
# |TRAASBB128F92E5354|[0.06,0.002845,192.3,0.07266,0.05151,6.839,0.003181,1638.0,0.5121]   |Pop_Rock     |
# +------------------+---------------------------------------------------------------------+-------------+
# only showing top 20 rows



#Question 2b

feature_genre = feature_genre.withColumn('genre_electronic', F.when(F.col('Genre') == 'Electronic',1).otherwise(0))

feature_genre.show()
# +------------------+--------------------+-------------+----------------+
# |          track_id|            Features|        Genre|genre_electronic|
# +------------------+--------------------+-------------+----------------+
# |TRAAABD128F429CF47|[0.0534,0.001519,...|     Pop_Rock|               0|
# |TRAABPK128F424CFDB|[0.05421,0.004769...|     Pop_Rock|               0|
# |TRAACER128F4290F96|[0.05057,0.004536...|     Pop_Rock|               0|
# |TRAADYB128F92D7E73|[0.02008,0.002983...|         Jazz|               0|
# |TRAAGHM128EF35CF8E|[0.05034,0.005852...|   Electronic|               1|
# |TRAAGRV128F93526C0|[0.03922,0.002131...|     Pop_Rock|               0|
# |TRAAGTO128F1497E3C|[0.03838,0.001236...|     Pop_Rock|               0|
# |TRAAHAU128F9313A3D|[0.05708,8.682E-4...|     Pop_Rock|               0|
# |TRAAHEG128E07861C3|[0.08356,0.0088,2...|          Rap|               0|
# |TRAAHZP12903CA25F4|[0.07767,0.002841...|          Rap|               0|
# |TRAAICW128F1496C68|[0.03825,0.002125...|International|               0|
# |TRAAJJW12903CBDDCB|[0.09538,0.01063,...|International|               0|
# |TRAAKLX128F934CEE4|[0.08041,0.004415...|   Electronic|               1|
# |TRAAKWR128F931B29F|[0.03473,7.117E-4...|     Pop_Rock|               0|
# |TRAALQN128E07931A4|[0.07281,0.007399...|   Electronic|               1|
# |TRAAMFF12903CE8107|[0.02975,0.005098...|     Pop_Rock|               0|
# |TRAAMHG128F92ED7B2|[0.04798,0.002646...|International|               0|
# |TRAAROH128F42604B0|[0.08735,0.003288...|   Electronic|               1|
# |TRAARQN128E07894DF|[0.08521,0.005002...|     Pop_Rock|               0|
# |TRAASBB128F92E5354|[0.06,0.002845,19...|     Pop_Rock|               0|
# +------------------+--------------------+-------------+----------------+
# only showing top 20 rows

feature_genre.groupBy('genre_electronic').count().show()
# +----------------+------+
# |genre_electronic| count|
# +----------------+------+
# |               1| 40026|
# |               0|373263|
# +----------------+------+



#Question 2c
#the following code is written by James Williams
from pyspark.sql.window import *

temp = (
  feature_genre
  .withColumn("id", F.monotonically_increasing_id())
  .withColumn("Random", F.rand())
  .withColumn("Row", F.row_number()
    .over(
      Window
      .partitionBy("genre_electronic")
      .orderBy("Random")
    )
  )
)

training = temp.where(
  ((F.col("genre_electronic") == 0) & (F.col("Row") < 373263 * 0.8)) |
  ((F.col("genre_electronic") == 1) & (F.col("Row") < 40026 * 0.8))
)

test = temp.join(training, on="id", how="left_anti")

training = training.drop("id", "Random", "Row")
test = test.drop("id", "Random", "Row")

training.groupBy('genre_electronic').count().show()
# +----------------+------+
# |genre_electronic| count|
# +----------------+------+
# |               1| 32020|
# |               0|298610|
# +----------------+------+

test.groupBy('genre_electronic').count().show()
# +----------------+-----+
# |genre_electronic|count|
# +----------------+-----+
# |               1| 8006|
# |               0|74653|
# +----------------+-----+

training.cache()
test.cache()


# oversampling
ratio = 15
n = 20
p = ratio / n

def random_resample(x, n, p):

    if x == 0:
        return [0]  # no sampling
    if x == 1:
        return list(range((np.sum(np.random.random(n) > p))))  # upsampling
    return []  # drop

random_resample_udf = udf(lambda x: random_resample(x, n, p), ArrayType(IntegerType()))

training_resampled = (
    training
    .withColumn("Sample", random_resample_udf(col("genre_electronic")))
    .select(
        col("track_id"),
        col("Features"),
        col("Genre"),
        col("genre_electronic"),
        explode(col("Sample")).alias("Sample")
    )
    .drop("Sample")
)

training_resampled.groupBy('genre_electronic').count().show()
# +----------------+------+
# |genre_electronic| count|
# +----------------+------+
# |               1|160543|
# |               0|298610|
# +----------------+------+

training_resampled.cache()

#Question 2d

#Logistic Regression
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(featuresCol='Features', labelCol='genre_electronic')
lr_model = lr.fit(training_resampled)
lr_predictions = lr_model.transform(test)


#Random Forest
from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(seed=1, featuresCol='Features', labelCol='genre_electronic')
rf_model = rf.fit(training_resampled)
rf_predictions = rf_model.transform(test)


#SVM
from pyspark.ml.classification import LinearSVC
svm = LinearSVC(labelCol='genre_electronic', featuresCol='Features')
svm_model = svm.fit(training_resampled)
svm_predictions = svm_model.transform(test)



#Question 2e
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def print_binary_metrics(predictions, labelCol="genre_electronic", predictionCol="prediction", rawPredictionCol="rawPrediction"):

  total = predictions.count()
  positive = predictions.filter((F.col(labelCol) == 1)).count()
  negative = predictions.filter((F.col(labelCol) == 0)).count()
  nP = predictions.filter((F.col(predictionCol) == 1)).count()
  nN = predictions.filter((F.col(predictionCol) == 0)).count()
  TP = predictions.filter((F.col(predictionCol) == 1) & (F.col(labelCol) == 1)).count()
  FP = predictions.filter((F.col(predictionCol) == 1) & (F.col(labelCol) == 0)).count()
  FN = predictions.filter((F.col(predictionCol) == 0) & (F.col(labelCol) == 1)).count()
  TN = predictions.filter((F.col(predictionCol) == 0) & (F.col(labelCol) == 0)).count()

  binary_evaluator = BinaryClassificationEvaluator(rawPredictionCol=rawPredictionCol, labelCol=labelCol, metricName="areaUnderROC")
  auroc = binary_evaluator.evaluate(predictions)

  print('actual total:    {}'.format(total))
  print('actual positive: {}'.format(positive))
  print('actual negative: {}'.format(negative))
  print('nP:              {}'.format(nP))
  print('nN:              {}'.format(nN))
  print('TP:              {}'.format(TP))
  print('FP:              {}'.format(FP))
  print('FN:              {}'.format(FN))
  print('TN:              {}'.format(TN))
  print('precision:       {}'.format(TP / (TP + FP)))
  print('recall:          {}'.format(TP / (TP + FN)))
  print('accuracy:        {}'.format((TP + TN) / total))
  print('auroc:           {}'.format(auroc))


print_binary_metrics(lr_predictions)
# actual total:    82659
# actual positive: 8006
# actual negative: 74653
# nP:              10328
# nN:              72331
# TP:              3315
# FP:              7013
# FN:              4691
# TN:              67640
# precision:       0.3209721146398141
# recall:          0.41406445166125405
# accuracy:        0.8584062231577929
# auroc:           0.7476483879237574


print_binary_metrics(rf_predictions)
# actual total:    82659
# actual positive: 8006
# actual negative: 74653
# nP:              11224
# nN:              71435
# TP:              3534
# FP:              7690
# FN:              4472
# TN:              66963
# precision:       0.3148610121168924
# recall:          0.4414189357981514
# accuracy:        0.8528653867092513
# auroc:           0.7692119583573943


print_binary_metrics(svm_predictions)
# actual total:    82659
# actual positive: 8006
# actual negative: 74653
# nP:              9615
# nN:              73044
# TP:              2936
# FP:              6679
# FN:              5070
# TN:              67974
# precision:       0.30535621424856996
# recall:          0.3667249562827879
# accuracy:        0.8578618178298794
# auroc:           0.7347824655532834




#Question 2f

#Logistic Regression
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(featuresCol='Features', labelCol='genre_electronic')
lr_model = lr.fit(training)
lr_predictions = lr_model.transform(test)
print_binary_metrics(lr_predictions)
# actual total:    82659
# actual positive: 8006
# actual negative: 74653
# nP:              570
# nN:              82089
# TP:              377
# FP:              193
# FN:              7629
# TN:              74460
# precision:       0.6614035087719298
# recall:          0.04708968273794654
# accuracy:        0.905370256112462
# auroc:           0.7455692915791301


#Random Forest
from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(seed=1, featuresCol='Features', labelCol='genre_electronic')
rf_model = rf.fit(training)
rf_predictions = rf_model.transform(test)
print_binary_metrics(rf_predictions)
# actual total:    82659
# actual positive: 8006
# actual negative: 74653
# nP:              292
# nN:              82367
# TP:              225
# FP:              67
# FN:              7781
# TN:              74586
# precision:       0.7705479452054794
# recall:          0.028103922058456157
# accuracy:        0.9050557108118898
# auroc:           0.7319709389458048


#SVM
from pyspark.ml.classification import LinearSVC
svm = LinearSVC(labelCol='genre_electronic', featuresCol='Features')
svm_model = svm.fit(training)
svm_predictions = svm_model.transform(test)
print_binary_metrics(svm_predictions)
#precision was unable to be calculated as no positive cases were predicted, causing a division by zero error.
# actual total:    82659
# actual positive: 8006
# actual negative: 74653
# nP:              0
# nN:              82659
# TP:              0
# FP:              0
# FN:              8006
# TN:              74653
# recall:          0.0
# accuracy:        0.9031442432161048
# auroc:           0.6783770414657493




#Question 3b

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(featuresCol='Features', labelCol='genre_electronic')
lr_grid = (ParamGridBuilder()
           .addGrid(lr.maxIter, [110, 120])
           .addGrid(lr.regParam, [0.2, 0.3])
           .addGrid(lr.aggregationDepth, [3, 4])
           .build())

from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(seed=1, featuresCol='Features', labelCol='genre_electronic')
rf_grid = (ParamGridBuilder()
           .addGrid(rf.impurity, ['gini', 'entropy'])
           .addGrid(rf.maxBins, [28, 32, 36])
           .addGrid(rf.maxDepth, [25, 30])
           .addGrid(rf.numTrees, [25, 30])
           .build())



roc = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="genre_electronic", metricName="areaUnderROC")

rf_cv = CrossValidator(estimator = rf, estimatorParamMaps = rf_grid, evaluator = roc, numFolds = 5)
rf_cv_model = rf_cv.fit(training_resampled)
rf_cv_predictions = rf_cv_model.transform(test)
print_binary_metrics(rf_cv_predictions)
# actual total:    372058
# actual positive: 8006
# actual negative: 74653
# nP:              31633
# nN:              340425
# TP:              1812
# FP:              1558
# FN:              6194
# TN:              73095
# precision:       0.5376854599406529
# recall:          0.22633025231076692
# accuracy:        0.201331512828645
# auroc:           0.8029491164749688


lr_cv = CrossValidator(estimator = lr, estimatorParamMaps = lr_grid, evaluator = roc, numFolds = 5)
lr_cv_model = lr_cv.fit(training_resampled)
lr_cv_predictions = lr_cv_model.transform(test)
print_binary_metrics(lr_cv_predictions)
# actual total:    82659
# actual positive: 8006
# actual negative: 74653
# nP:              11155
# nN:              71504
# TP:              3493
# FP:              7662
# FN:              4513
# TN:              66991
# precision:       0.3131331241595697
# recall:          0.43629777666749936
# accuracy:        0.8527081140589652
# auroc:           0.7731637552025658




#Question 4b
from pyspark.ml.feature import StringIndexer

feature_genre_m = feature_genre
feature_genre_m = feature_genre_m.drop("genre_electronic")

indexer = StringIndexer(inputCol="Genre", outputCol="genre_index")
indexer_model = indexer.fit(feature_genre_m)
mult_index = indexer_model.transform(feature_genre_m)

mult_index.show()
# +------------------+--------------------+-------------+-----------+
# |          track_id|            Features|        Genre|genre_index|
# +------------------+--------------------+-------------+-----------+
# |TRAAABD128F429CF47|[0.0534,0.001519,...|     Pop_Rock|        0.0|
# |TRAABPK128F424CFDB|[0.05421,0.004769...|     Pop_Rock|        0.0|
# |TRAACER128F4290F96|[0.05057,0.004536...|     Pop_Rock|        0.0|
# |TRAADYB128F92D7E73|[0.02008,0.002983...|         Jazz|        3.0|
# |TRAAGHM128EF35CF8E|[0.05034,0.005852...|   Electronic|        1.0|
# |TRAAGRV128F93526C0|[0.03922,0.002131...|     Pop_Rock|        0.0|
# |TRAAGTO128F1497E3C|[0.03838,0.001236...|     Pop_Rock|        0.0|
# |TRAAHAU128F9313A3D|[0.05708,8.682E-4...|     Pop_Rock|        0.0|
# |TRAAHEG128E07861C3|[0.08356,0.0088,2...|          Rap|        2.0|
# |TRAAHZP12903CA25F4|[0.07767,0.002841...|          Rap|        2.0|
# |TRAAICW128F1496C68|[0.03825,0.002125...|International|        5.0|
# |TRAAJJW12903CBDDCB|[0.09538,0.01063,...|International|        5.0|
# |TRAAKLX128F934CEE4|[0.08041,0.004415...|   Electronic|        1.0|
# |TRAAKWR128F931B29F|[0.03473,7.117E-4...|     Pop_Rock|        0.0|
# |TRAALQN128E07931A4|[0.07281,0.007399...|   Electronic|        1.0|
# |TRAAMFF12903CE8107|[0.02975,0.005098...|     Pop_Rock|        0.0|
# |TRAAMHG128F92ED7B2|[0.04798,0.002646...|International|        5.0|
# |TRAAROH128F42604B0|[0.08735,0.003288...|   Electronic|        1.0|
# |TRAARQN128E07894DF|[0.08521,0.005002...|     Pop_Rock|        0.0|
# |TRAASBB128F92E5354|[0.06,0.002845,19...|     Pop_Rock|        0.0|
# +------------------+--------------------+-------------+-----------+
# only showing top 20 rows


mult_index.select('Genre', 'genre_index').distinct().orderBy('genre_index').show(22)
# +--------------+-----------+
# |         Genre|genre_index|
# +--------------+-----------+
# |      Pop_Rock|        0.0|
# |    Electronic|        1.0|
# |           Rap|        2.0|
# |          Jazz|        3.0|
# |         Latin|        4.0|
# | International|        5.0|
# |           RnB|        6.0|
# |       Country|        7.0|
# |     Religious|        8.0|
# |        Reggae|        9.0|
# |         Blues|       10.0|
# |         Vocal|       11.0|
# |          Folk|       12.0|
# |       New Age|       13.0|
# | Comedy_Spoken|       14.0|
# |         Stage|       15.0|
# |Easy_Listening|       16.0|
# |   Avant_Garde|       17.0|
# |     Classical|       18.0|
# |      Children|       19.0|
# |       Holiday|       20.0|
# +--------------+-----------+

mult_index.count()
# 413289



#Question 4c

temp_m = (
  mult_index
  .withColumn("id", F.monotonically_increasing_id())
  .withColumn("Random", F.rand())
  .withColumn("Row", F.row_number()
    .over(
      Window
      .partitionBy("genre_index")
      .orderBy("Random")
    )
  )
)

class_counts = (
  mult_index
  .groupBy("genre_index")
  .count()
  .toPandas()
  .set_index("genre_index")["count"]
  .to_dict()
)
classes = sorted(class_counts.keys())

training_m = temp_m
for c in classes:
  training_m = training_m.where((col("genre_index") != c) | (col("Row") < class_counts[c] * 0.8))

test_m = temp_m.join(training_m, on="id", how="left_anti")

training_m = training_m.drop("id", "Random", "Row")
test_m = test_m.drop("id", "Random", "Row")

training_m.groupBy('genre_index').count().orderBy('count').show(22)
# +-----------+------+
# |genre_index| count|
# +-----------+------+
# |       20.0|   158|
# |       19.0|   365|
# |       18.0|   432|
# |       17.0|   798|
# |       16.0|  1218|
# |       15.0|  1282|
# |       14.0|  1640|
# |       13.0|  3139|
# |       12.0|  4560|
# |       11.0|  4851|
# |       10.0|  5392|
# |        9.0|  5495|
# |        8.0|  6975|
# |        7.0|  9127|
# |        6.0| 11083|
# |        5.0| 11237|
# |        4.0| 13911|
# |        3.0| 14088|
# |        2.0| 16452|
# |        1.0| 32020|
# |        0.0|186395|
# +-----------+------+

test_m.groupBy('genre_index').count().orderBy('count').show(22)
# +-----------+-----+
# |genre_index|count|
# +-----------+-----+
# |       20.0|   40|
# |       19.0|   92|
# |       18.0|  109|
# |       17.0|  200|
# |       16.0|  305|
# |       15.0|  321|
# |       14.0|  411|
# |       13.0|  786|
# |       12.0| 1141|
# |       11.0| 1213|
# |       10.0| 1349|
# |        9.0| 1375|
# |        8.0| 1745|
# |        7.0| 2282|
# |        6.0| 2771|
# |        5.0| 2810|
# |        4.0| 3478|
# |        3.0| 3523|
# |        2.0| 4114|
# |        1.0| 8006|
# |        0.0|46600|
# +-----------+-----+

training_m.cache()
test_m.cache()


from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def print_mult_metrics(predictions, genre_num, labelCol="genre_index", predictionCol="prediction", rawPredictionCol="rawPrediction"):

  total = predictions.count()
  positive = predictions.filter((F.col(labelCol) == genre_num)).count()
  negative = predictions.filter((F.col(labelCol) != genre_num)).count()
  nP = predictions.filter((F.col(predictionCol) == genre_num)).count()
  nN = predictions.filter((F.col(predictionCol) != genre_num)).count()
  TP = predictions.filter((F.col(predictionCol) == genre_num) & (F.col(labelCol) == genre_num)).count()
  FP = predictions.filter((F.col(predictionCol) == genre_num) & (F.col(labelCol) != genre_num)).count()
  FN = predictions.filter((F.col(predictionCol) != genre_num) & (F.col(labelCol) == genre_num)).count()
  TN = predictions.filter((F.col(predictionCol) != genre_num) & (F.col(labelCol) != genre_num)).count()

  mult_evaluator = MulticlassClassificationEvaluator(predictionCol=predictionCol,
                                                     labelCol=labelCol, metricName="f1")
  f1 = mult_evaluator.evaluate(predictions)

  mult_evaluator2 = MulticlassClassificationEvaluator(predictionCol=predictionCol,
                                                      labelCol=labelCol, metricName="weightedPrecision")
  weightedPrecision = mult_evaluator2.evaluate(predictions)

  mult_evaluator3 = MulticlassClassificationEvaluator(predictionCol=predictionCol,
                                                      labelCol=labelCol, metricName="weightedRecall")
  weightedRecall = mult_evaluator3.evaluate(predictions)

  print('actual total:       {}'.format(total))
  print('actual positive:    {}'.format(positive))
  print('actual negative:    {}'.format(negative))
  print('nP:                 {}'.format(nP))
  print('nN:                 {}'.format(nN))
  print('TP:                 {}'.format(TP))
  print('FP:                 {}'.format(FP))
  print('FN:                 {}'.format(FN))
  print('TN:                 {}'.format(TN))
  print('precision:          {}'.format(TP / (TP + FP)))
  print('recall:             {}'.format(TP / (TP + FN)))
  print('weighted precision: {}'.format(weightedPrecision))
  print('weighted recall:    {}'.format(weightedRecall))
  print('accuracy:           {}'.format((TP + TN) / total))
  print('f1:                 {}'.format(f1))


#without resampling
from pyspark.ml.classification import RandomForestClassifier
rf_m = RandomForestClassifier(seed=1, featuresCol='Features', labelCol='genre_index')
rf_model_m = rf_m.fit(training_m)
rf_predictions_m = rf_model_m.transform(test_m)

print_mult_metrics(rf_predictions_m, 1)

# actual total:       82671
# actual positive:    8006
# actual negative:    74665
# nP:                 1251
# nN:                 81420
# TP:                 693
# FP:                 558
# FN:                 7313
# TN:                 74107
# precision:          0.5539568345323741
# recall:             0.08656007994004497
# weighted precision: 0.3996722064278142
# weighted recall:    0.5739255603536911
# accuracy:           0.9047912811022003
# f1:                 0.4367069201338815



#to resample
count_upper_bound = 100000
count_lower_bound = 1000
class_counts = {label: count for label, count in training_m.groupBy("genre_index").count().collect()}

def random_resample(x, class_counts, count_upper_bound, count_lower_bound):

    count = class_counts[x]

    if count > count_upper_bound:
        if np.random.rand() < count_upper_bound / count:
            return [x]
        else:
            return []

    if count < count_lower_bound:
        return [x] * int(1 + np.random.poisson((count_lower_bound - count) / count))

    return [x]

random_resample_udf = udf(lambda x: random_resample(x, class_counts, count_upper_bound, count_lower_bound), ArrayType(IntegerType()))

training_m_resampled = (
    training_m
    .withColumn("Sample", random_resample_udf(col("genre_index")))
    .select(
        col("track_id"),
        col("Features"),
        col("genre_index"),
        explode(col("Sample")).alias("Sample")
    )
    .drop("Sample")
)

training_m_resampled.groupBy('genre_index').count().orderBy('count').show(22)
# +-----------+-----+
# |genre_index|count|
# +-----------+-----+
# |       18.0|  988|
# |       17.0|  998|
# |       19.0| 1037|
# |       20.0| 1057|
# |       16.0| 1218|
# |       15.0| 1282|
# |       14.0| 1640|
# |       13.0| 3139|
# |       12.0| 4560|
# |       11.0| 4851|
# |       10.0| 5392|
# |        9.0| 5495|
# |        8.0| 6975|
# |        7.0| 9127|
# |        6.0|11083|
# |        5.0|11237|
# |        4.0|13911|
# |        3.0|14088|
# |        2.0|16452|
# |        1.0|32020|
# |        0.0|66814|
# +-----------+-----+


#resampled training data
from pyspark.ml.classification import RandomForestClassifier
rf_m = RandomForestClassifier(seed=1, featuresCol='Features', labelCol='genre_index')
rf_model_m_r = rf_m.fit(training_m_resampled)
rf_predictions_m_r = rf_model_m_r.transform(test_m)

print_mult_metrics(rf_predictions_m_r, 1)

# actual total:       82671
# actual positive:    8006
# actual negative:    74665
# nP:                 7448
# nN:                 75223
# TP:                 2452
# FP:                 4996
# FN:                 5554
# TN:                 69669
# precision:          0.3292158968850698
# recall:             0.3062702972770422
# weighted precision: 0.40697263014607155
# weighted recall:    0.5625551886393052
# accuracy:           0.8723857217162003
# f1:                 0.4699696840453606



y_true = rf_predictions_m_r.select(['genre_index']).collect()
y_pred = rf_predictions_m_r.select(['prediction']).collect()

from sklearn.metrics import confusion_matrix
print(confusion_matrix(y_true, y_pred))

# [[42078  2479   923  1120     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [ 4617  2452   736   201     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [ 1680  1244  1185     5     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [ 2578   119    34   792     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [ 3067   242   103    66     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [ 2280   214   102   214     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [ 2105   251   344    71     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [ 2117    34     6   125     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [ 1590    46    51    58     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [  975   244   150     6     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [ 1177    32     4   136     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [  982     4     1   226     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [  932    21     0   188     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [  468    17     6   295     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [  378    25     5     3     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [  189     6     1   125     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [  223     3     0    79     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [  140    12     3    45     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [   53     1     0    55     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [   80     2     0    10     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]
#  [   32     0     0     8     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0     0]]
#



#--------------------------------------------------------------------------------------------------------




#SONG RECOOMENDATIONS

#Question 1a
triplets_not_mismatched.select(F.countDistinct('song_id')).show()
# +-----------------------+
# |count(DISTINCT song_id)|
# +-----------------------+
# |                 378310|
# +-----------------------+


triplets_not_mismatched.select(F.countDistinct('user_id')).show()
# +-----------------------+
# |count(DISTINCT user_id)|
# +-----------------------+
# |                1019318|
# +-----------------------+



#Question 1b

user_activity = triplets_not_mismatched.groupBy('user_id')\
    .agg(F.countDistinct('song_id').alias('total_songs'),
         F.sum('plays').alias('total_plays'))\
    .orderBy(F.col('total_plays').desc())

user_activity.show(10,False)
# +----------------------------------------+-----------+-----------+
# |user_id                                 |total_songs|total_plays|
# +----------------------------------------+-----------+-----------+
# |093cb74eb3c517c5179ae24caf0ebec51b24d2a2|195        |13074      |
# |119b7c88d58d0c6eb051365c103da5caf817bea6|1362       |9104       |
# |3fa44653315697f42410a30cb766a4eb102080bb|146        |8025       |
# |a2679496cd0af9779a92a13ff7c6af5c81ea8c7b|518        |6506       |
# |d7d2d888ae04d16e994d6964214a1de81392ee04|1257       |6190       |
# |4ae01afa8f2430ea0704d502bc7b57fb52164882|453        |6153       |
# |b7c24f770be6b802805ac0e2106624a517643c17|1364       |5827       |
# |113255a012b2affeab62607563d03fbdf31b08e7|1096       |5471       |
# |99ac3d883681e21ea68071019dba828ce76fe94d|939        |5385       |
# |6d625c6557df84b60d90426c0116138b617b9449|1307       |5362       |
# +----------------------------------------+-----------+-----------+
# only showing top 10 rows

user_activity.orderBy(F.col('total_songs').desc()).show(10,False)
# +----------------------------------------+-----------+-----------+
# |user_id                                 |total_songs|total_plays|
# +----------------------------------------+-----------+-----------+
# |ec6dfcf19485cb011e0b22637075037aae34cf26|4316       |5146       |
# |8cb51abc6bf8ea29341cb070fe1e1af5e4c3ffcc|1562       |2599       |
# |5a3417a1955d9136413e0d293cd36497f5e00238|1557       |1679       |
# |fef771ab021c200187a419f5e55311390f850a50|1545       |2847       |
# |c1255748c06ee3f6440c51c439446886c7807095|1498       |4977       |
# |4e73d9e058d2b1f2dba9c1fe4a8f416f9f58364f|1470       |4285       |
# |cbc7bddbe3b2f59fdbe031b3c8d0db4175d361e6|1457       |2483       |
# |96f7b4f800cafef33eae71a6bc44f7139f63cd7a|1407       |1996       |
# |b7c24f770be6b802805ac0e2106624a517643c17|1364       |5827       |
# |119b7c88d58d0c6eb051365c103da5caf817bea6|1362       |9104       |
# +----------------------------------------+-----------+-----------+
# only showing top 10 rows


user_activity.approxQuantile('total_songs', [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)
# [3.0, 14.0, 25.0, 50.0, 4316.0]


user_activity.approxQuantile('total_plays', [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)
# [3.0, 30.0, 67.0, 151.0, 13074.0]



#Question 1c
song_popularity = triplets_not_mismatched.groupBy('song_id')\
    .agg(F.sum('plays').alias('total_play'),
         F.countDistinct('user_id').alias('users_played'))\
    .orderBy(F.col('total_play').desc())

song_popularity.show()
# +------------------+----------+------------+
# |           song_id|total_play|users_played|
# +------------------+----------+------------+
# |SOBONKR12A58A7A7E0|    726885|       84000|
# |SOSXLTC12AF72A7F54|    527893|       80656|
# |SOEGIYH12A6D4FC0E3|    389880|       69487|
# |SOAXGDH12A8C13F8A1|    356533|       90444|
# |SONYKOW12AB01849C9|    292642|       78353|
# |SOPUCYA12A8C13A694|    274627|       46078|
# |SOUFTBI12AB0183F65|    268353|       37642|
# |SOVDSJC12A58A7A271|    244730|       36976|
# |SOOFYTN12A6D4F9B35|    241669|       40403|
# |SOHTKMO12AB01843B0|    236494|       46077|
# |SOBOUPA12A6D4F81F1|    225652|       41093|
# |SOLFXKT12AB017E3E0|    197181|       64229|
# |SOTCMDJ12A6D4F8528|    192884|       32015|
# |SOFLJQZ12A6D4FADA6|    185653|       58610|
# |SOTWNDJ12A8C143984|    174080|       47011|
# |SOUNZHU12A8AE47481|    158636|       30841|
# |SOUVTSM12AC468F6A7|    155717|       51022|
# |SOUSMXX12AB0185C24|    155529|       53260|
# |SOUDLVN12AAFF43658|    146978|       19842|
# |SOWCKVR12A8C142411|    145725|       52080|
# +------------------+----------+------------+
# only showing top 20 rows


import matplotlib.pyplot as plt

user_song_plot = user_activity.toPandas()
user_song_plot.hist(column = "total_songs", bins = 50)
plt.xlabel("Unique Songs", fontsize = 10)
plt.ylabel("Number of Users", fontsize = 10)
plt.title("Distribution of Number of Songs Played")
plt.tight_layout()
plt.savefig(f"user_song_plot.jpg", bbox_inches = "tight")

user_play_plot = user_activity.toPandas()
user_play_plot.hist(column = "total_plays", bins = 50)
plt.xlabel("Number of Plays", fontsize = 10)
plt.ylabel("Number of Users", fontsize = 10)
plt.title("Distribution of Number of Plays")
plt.tight_layout()
plt.savefig(f"user_play_plot.jpg", bbox_inches = "tight")

song_play_plot = song_popularity.toPandas()
song_play_plot.hist(column = "total_play", bins = 30)
plt.xlabel("Number of Plays", fontsize = 10)
plt.ylabel("Number of Songs", fontsize = 10)
plt.title("Distribution of Number of Plays")
plt.tight_layout()
plt.savefig(f"song_play_plot.jpg", bbox_inches = "tight")

song_user_plot = song_popularity.toPandas()
song_user_plot.hist(column = "users_played", bins = 50)
plt.xlabel("Number of Users", fontsize = 10)
plt.ylabel("Number of Songs", fontsize = 10)
plt.title("Distribution of Number of Plays")
plt.tight_layout()
plt.savefig(f"song_user_plot.jpg", bbox_inches = "tight")




#Question 1d

for plays in (1,2,3,4,5,6,7,8,9,10):
    total_songs = 378310
    songs = song_popularity.filter(F.col('total_play')<= plays).count()
    percentage = (songs/total_songs)*100
    print('times played:    {}'.format(plays))
    print('songs:           {}'.format(songs))
    print('percentage:      {:.2f}%'.format(percentage))
    print('')

# times played:    2
# songs:           38107
# percentage:      10.07%
#
# times played:    3
# songs:           51377
# percentage:      13.58%
#
# times played:    4
# songs:           62442
# percentage:      16.51%
#
# times played:    5
# songs:           72973
# percentage:      19.29%
#
# times played:    6
# songs:           82388
# percentage:      21.78%
#
# times played:    7
# songs:           90750
# percentage:      23.99%
#
# times played:    8
# songs:           98332
# percentage:      25.99%
#
# times played:    9
# songs:           105242
# percentage:      27.82%
#
# times played:    10
# songs:           111783
# percentage:      29.55%


for songs in (10,11,12,13,14,15,16):
    total_users = 1019318
    users = user_activity.filter(F.col('total_songs')<= songs).count()
    percentage = (users/total_users)*100
    print('songs played:    {}'.format(songs))
    print('users:           {}'.format(users))
    print('percentage:      {:.2f}%'.format(percentage))
    print('')

# songs played:    10
# users:           79843
# percentage:      7.83%
#
# songs played:    11
# users:           125680
# percentage:      12.33%
#
# songs played:    12
# users:           167690
# percentage:      16.45%
#
# songs played:    13
# users:           206329
# percentage:      20.24%
#
# songs played:    14
# users:           241617
# percentage:      23.70%
#
# songs played:    15
# users:           274408
# percentage:      26.92%
#
# songs played:    16
# users:           304671
# percentage:      29.89%



#cleaning data
N = 10
M = 16

songs_cleaned = song_popularity.filter(F.col('total_play') > N)
users_cleaned = user_activity.filter(F.col('total_songs') > M)

triplets_cleaned = triplets_not_mismatched.join(songs_cleaned, on='song_id', how='inner')
triplets_cleaned = triplets_cleaned.drop('total_play', 'users_played')
triplets_cleaned = triplets_not_mismatched.join(users_cleaned, on='user_id', how='inner')
triplets_cleaned = triplets_cleaned.drop('total_songs', 'total_plays')

triplets_not_mismatched.count()
# 45795111

triplets_cleaned.count()
# 42054691



#Question 1e

from pyspark.ml.feature import VectorAssembler, StringIndexer

user_indexer = StringIndexer(inputCol="user_id", outputCol="user_index")
user_model = user_indexer.fit(triplets_cleaned)
triplets_cleaned = user_model.transform(triplets_cleaned)

song_indexer = StringIndexer(inputCol="song_id", outputCol="song_index")
song_model = song_indexer.fit(triplets_cleaned)
triplets_cleaned = song_model.transform(triplets_cleaned)

triplets_cleaned.show()
# +--------------------+------------------+-----+----------+----------+
# |             user_id|           song_id|plays|user_index|song_index|
# +--------------------+------------------+-----+----------+----------+
# |00007ed2509128dcd...|SOBJYFB12AB018372D|    1|  229068.0|    6591.0|
# |00007ed2509128dcd...|SOBRMPB12A58A7F55A|    2|  229068.0|   91532.0|
# |00007ed2509128dcd...|SOICJAD12A8C13B2F4|    1|  229068.0|   35551.0|
# |00007ed2509128dcd...|SOIZRWS12AF72A1163|    1|  229068.0|   44335.0|
# |00007ed2509128dcd...|SOULNCS12A8C13C163|    1|  229068.0|  179426.0|
# |00007ed2509128dcd...|SOEKSKB12A6D4F7938|    1|  229068.0|    2960.0|
# |00007ed2509128dcd...|SOQPGMT12AF72A0865|    1|  229068.0|   48820.0|
# |00007ed2509128dcd...|SOKHEEY12A8C1418FE|    2|  229068.0|    5002.0|
# |00007ed2509128dcd...|SOOGCBL12A8C13FA4E|    2|  229068.0|   64646.0|
# |00007ed2509128dcd...|SOVMWUC12A8C13750B|    1|  229068.0|     694.0|
# |00007ed2509128dcd...|SOWXPFM12A8C13B2EC|    1|  229068.0|   48339.0|
# |00007ed2509128dcd...|SOAYETG12A67ADA751|    2|  229068.0|     574.0|
# |00007ed2509128dcd...|SODTGOI12A8C13EBE8|    4|  229068.0|   10569.0|
# |00007ed2509128dcd...|SOHBYIJ12A6D4FB344|    1|  229068.0|   49995.0|
# |00007ed2509128dcd...|SOLJSMV12A8C13B2D9|    1|  229068.0|   46364.0|
# |00007ed2509128dcd...|SOWYFRZ12A6D4FD507|    1|  229068.0|   66196.0|
# |00007ed2509128dcd...|SOELPFP12A58A7DA4F|    1|  229068.0|   50735.0|
# |00007ed2509128dcd...|SOGEWRX12AB0189432|    2|  229068.0|   45295.0|
# |00007ed2509128dcd...|SOHEWBM12A58A7922A|    1|  229068.0|   72825.0|
# |00007ed2509128dcd...|SOIITTN12A6D4FD74D|    1|  229068.0|   27226.0|
# +--------------------+------------------+-----+----------+----------+
# only showing top 20 rows


(training_sr, test_sr) = triplets_cleaned.randomSplit([0.7, 0.3], seed=50)
users_notInTrain = test_sr.join(training_sr, on='user_id', how='left_anti')

print(f"training                = {training_sr.count()}")
print(f"test                    = {test_sr.count()}")
print(f"users_not_in_training   = {users_notInTrain.count()}")

# training                = 29439090
# test                    = 12615601
# users_not_in_training   = 0


#if there are unseen users in the test data

# counts = users_notInTrain.groupBy("user_id").count().toPandas().set_index("user_id")["count"].to_dict()
#
# temp = (
#   users_notInTrain
#   .withColumn("id", monotonically_increasing_id())
#   .withColumn("random", rand())
#   .withColumn(
#     "row",
#     row_number()
#     .over(
#       Window
#       .partitionBy("user_id")
#       .orderBy("random")
#     )
#   )
# )
#
# for k, v in counts.items():
#   temp = temp.where((col("user_id") != k) | (col("row") < v * 0.7))
#
# temp = temp.drop("id", "random", "row")
#
# training = training.union(temp.select(training.columns))
# test = test.join(temp, on=["user_id", "song_id"], how="left_anti")
# users_notInTrain = test.join(training, on="user_id", how="left_anti")




#Question 2a

from pyspark.ml.recommendation import ALS

als = ALS(maxIter=5, regParam=0.1, userCol="user_index", itemCol="song_index", ratingCol="plays", implicitPrefs=True)
alsModel = als.fit(training_sr)
predictions = alsModel.transform(test_sr)

predictions.show()
# +--------------------+------------------+-----+----------+----------+----------+
# |             user_id|           song_id|plays|user_index|song_index|prediction|
# +--------------------+------------------+-----+----------+----------+----------+
# |b7032f457c624e231...|SOTWNDJ12A8C143984|    1|     218.0|      12.0| 0.3053663|
# |1f91494ea547a8eb6...|SOTWNDJ12A8C143984|    1|     613.0|      12.0| 0.3640174|
# |6804f9a5c6da8e7eb...|SOTWNDJ12A8C143984|    1|     857.0|      12.0|0.26404616|
# |7a519e5cfb193318c...|SOTWNDJ12A8C143984|    1|    1521.0|      12.0|  0.302116|
# |656affaed3ccfc63b...|SOTWNDJ12A8C143984|    1|    2004.0|      12.0|0.42472458|
# |ec870661f26b44f99...|SOTWNDJ12A8C143984|    1|    2115.0|      12.0| 0.7570898|
# |3166206d16c682a58...|SOTWNDJ12A8C143984|    1|    2337.0|      12.0|0.25413606|
# |20993e2241434d35f...|SOTWNDJ12A8C143984|    2|    2737.0|      12.0|0.40553948|
# |4c02523b662af25e4...|SOTWNDJ12A8C143984|    1|    2968.0|      12.0| 0.5281171|
# |1bb4a139325fceac6...|SOTWNDJ12A8C143984|    1|    5232.0|      12.0| 0.3810187|
# |e6691cf5f8296dc76...|SOTWNDJ12A8C143984|    3|    5318.0|      12.0|0.77754146|
# |59b7ac833f4fcd3a1...|SOTWNDJ12A8C143984|    3|    5399.0|      12.0| 0.8085496|
# |6239255ef73d66b6a...|SOTWNDJ12A8C143984|    7|    5612.0|      12.0|0.38466337|
# |e70c8e2dd2f4d9825...|SOTWNDJ12A8C143984|    1|    5941.0|      12.0|0.43390492|
# |bf22fb2dcb4f41333...|SOTWNDJ12A8C143984|    3|    6585.0|      12.0| 0.7107108|
# |1fd67d6a6063ba000...|SOTWNDJ12A8C143984|    1|    7942.0|      12.0|0.47712144|
# |81c444bcfd2a00e93...|SOTWNDJ12A8C143984|    5|    9523.0|      12.0|0.91538876|
# |a3bdb3238cb06811b...|SOTWNDJ12A8C143984|    4|    9737.0|      12.0| 0.5117765|
# |7f010f4f3c5c53408...|SOTWNDJ12A8C143984|    1|   10258.0|      12.0| 0.5286677|
# |669bf5ba483e6df7a...|SOTWNDJ12A8C143984|    1|   11044.0|      12.0| 0.6423178|
# +--------------------+------------------+-----+----------+----------+----------+
# only showing top 20 rows

predictions.count()
# 12615601




#Question 2b

k = 5
topK = alsModel.recommendForAllUsers(k)

def extract_songs_top_k(x, k):
  x = sorted(x, key=lambda i: -i[1])
  return [i[0] for i in x][0:k]

extract_songs_top_k_udf = F.udf(lambda x: extract_songs_top_k(x, k), ArrayType(IntegerType()))

recommended_songs = (
  topK
  .withColumn("recommended_songs", extract_songs_top_k_udf(F.col("recommendations")))
  .select("user_index", "recommended_songs")
)

user_index_chosen = [2675, 7128, 10245, 4805, 979, 25836, 346]
recommended_songs_chosen = recommended_songs.filter(F.col("user_index").isin(user_index_chosen))
recommended_songs_chosen.show(7, False)
# +----------+-------------------------+
# |user_index|recommended_songs        |
# +----------+-------------------------+
# |25836     |[261, 160, 193, 71, 113] |
# |2675      |[5, 0, 2, 7, 8]          |
# |346       |[34, 144, 131, 80, 72]   |
# |7128      |[180, 23, 1082, 88, 50]  |
# |4805      |[160, 261, 193, 168, 194]|
# |10245     |[52, 0, 39, 49, 74]      |
# |979       |[0, 10, 7, 16, 13]       |
# +----------+-------------------------+


songs_played = test_sr.groupBy(F.col('user_index')).agg(F.collect_set(F.col('song_index')).alias('songs'))
songs_played_chosen = songs_played.filter(F.col('user_index').isin(user_index_chosen))
songs_played_chosen.show(7,False)
# +----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# |user_index|songs                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
# +----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# |10245.0   |[2637.0, 172825.0, 103689.0, 2545.0, 1131.0, 10506.0, 17379.0, 6354.0, 397.0, 1331.0, 754.0, 1803.0, 24031.0, 166733.0, 20045.0, 1875.0, 56518.0, 100963.0, 5711.0, 2861.0, 1356.0, 8881.0, 1423.0, 25339.0, 548.0, 290.0, 615.0, 153.0, 2327.0, 13124.0, 3989.0, 7844.0, 25815.0, 48159.0, 49276.0, 5994.0, 36571.0, 18686.0, 30235.0, 318.0, 118.0, 66558.0, 16337.0, 151.0, 36094.0, 225924.0, 3156.0, 99203.0, 23552.0, 34747.0, 804.0, 1693.0, 35433.0, 15473.0, 38312.0, 73666.0, 24040.0, 423.0, 8590.0, 39212.0, 28476.0, 43899.0, 24261.0, 3708.0, 5762.0, 5167.0, 1625.0, 1774.0, 4919.0, 2772.0, 58610.0, 89428.0, 1060.0, 7893.0, 4159.0, 10863.0, 9303.0, 72.0]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
# |346.0     |[501.0, 1552.0, 33195.0, 8622.0, 2545.0, 2709.0, 1213.0, 4978.0, 12270.0, 14366.0, 1152.0, 975.0, 559.0, 11471.0, 8156.0, 813.0, 2092.0, 10153.0, 4747.0, 3630.0, 6820.0, 5034.0, 12180.0, 53092.0, 26601.0, 93195.0, 5770.0, 13208.0, 3701.0, 5873.0, 1191.0, 272.0, 13488.0, 1453.0, 43413.0, 25254.0, 2007.0, 187.0, 13038.0, 68830.0, 5233.0, 2786.0, 15338.0, 5132.0, 1832.0, 13612.0, 2950.0, 5603.0, 28927.0, 858.0, 64212.0, 794.0, 10097.0, 5129.0, 2704.0, 894.0, 16211.0, 845.0, 10137.0, 15353.0, 16946.0, 1369.0, 8884.0, 6831.0, 2978.0, 269.0, 1456.0, 7344.0, 32271.0, 12940.0, 351.0, 40283.0, 3154.0, 38448.0, 482.0, 10636.0, 1430.0, 5126.0, 84415.0, 226238.0, 56520.0, 15217.0, 10306.0, 3933.0, 229.0, 18688.0, 7565.0, 21781.0, 8397.0, 15690.0, 63126.0, 2196.0, 2833.0, 7977.0, 9461.0, 15953.0, 785.0, 1670.0, 5324.0, 7296.0, 54008.0, 2698.0, 19229.0, 1998.0, 98482.0, 7315.0, 14526.0, 12329.0, 1839.0, 3488.0, 23039.0, 2872.0, 790.0, 1972.0, 51368.0, 1290.0, 22776.0, 905.0, 330.0, 946.0, 35917.0, 1294.0, 159164.0, 1541.0, 2502.0, 1443.0, 28546.0, 2553.0, 1315.0, 1150.0, 33448.0, 2327.0, 24255.0, 86996.0, 1936.0, 12117.0, 2717.0, 23851.0, 6407.0, 503.0, 1155.0, 1109.0, 2439.0, 76189.0, 1760.0, 35446.0, 115198.0, 26833.0, 961.0, 71020.0, 34050.0, 889.0, 52312.0, 12725.0, 11045.0, 2643.0, 686.0, 43806.0, 34484.0, 4722.0, 40423.0, 4049.0, 5581.0, 2047.0, 786.0, 2118.0, 5684.0, 4470.0, 17823.0, 8939.0, 21012.0, 69834.0, 4121.0, 1225.0, 125.0, 783.0, 3472.0, 117.0, 19753.0, 1980.0, 2076.0, 7571.0, 37434.0, 10413.0, 3256.0, 10184.0, 13167.0, 279.0, 149957.0, 21972.0, 870.0, 806.0, 1517.0, 757.0, 16510.0, 2064.0, 5802.0, 798.0, 2699.0, 93158.0]|
# |4805.0    |[47353.0, 4566.0, 2607.0, 9295.0, 7091.0, 1521.0, 3879.0, 52807.0, 8309.0, 10756.0, 2287.0, 25126.0, 474.0, 32743.0, 10140.0, 55344.0, 1105.0, 47803.0, 46906.0, 45995.0, 29970.0, 17562.0, 13807.0, 8822.0, 12478.0, 3292.0, 14757.0, 4173.0, 10891.0, 6269.0, 7580.0, 15720.0, 4686.0, 148.0, 22341.0, 2882.0, 6884.0, 49164.0, 2296.0, 931.0, 3815.0, 8284.0, 25018.0, 21990.0, 24217.0, 34112.0, 26204.0, 87237.0, 19670.0, 951.0, 22718.0, 4335.0, 23828.0, 5564.0, 19281.0, 7268.0, 115556.0, 5316.0, 3813.0, 630.0, 3617.0, 3504.0, 11554.0, 4985.0, 663.0, 13522.0, 11907.0, 11390.0, 61672.0, 6117.0, 763.0, 153661.0, 20192.0, 235.0, 2199.0, 12723.0, 51128.0, 43847.0, 17430.0, 16104.0, 49656.0, 21420.0, 50499.0, 82070.0, 22308.0, 150795.0, 850.0, 21702.0, 11344.0, 19432.0, 5660.0, 71203.0, 48008.0, 17624.0, 19764.0, 4673.0, 12879.0, 6335.0, 4161.0, 6026.0, 4817.0, 893.0, 23540.0, 2248.0, 8580.0, 993.0, 15524.0, 93814.0, 107137.0, 1696.0, 17582.0, 49570.0, 17250.0]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
# |2675.0    |[1552.0, 392.0, 15297.0, 39847.0, 63.0, 11227.0, 388.0, 33.0, 8292.0, 105.0, 8433.0, 6577.0, 227.0, 479.0, 63674.0, 28108.0, 54320.0, 5241.0, 121.0, 4041.0, 9779.0, 112.0, 62.0, 58.0, 32.0, 153.0, 7108.0, 17.0, 6633.0, 11583.0, 3403.0, 94.0, 53.0, 49.0, 146.0, 16897.0, 84.0, 68.0, 185.0, 36.0, 11.0, 581.0, 167.0, 5396.0, 3.0, 7492.0, 845.0, 418.0, 48.0, 27.0, 14.0, 346.0, 1569.0, 1.0, 3359.0, 165.0, 116.0, 9738.0, 20782.0, 5125.0, 3122.0, 31.0, 145.0, 5.0, 14868.0, 13308.0, 993.0, 51736.0, 2803.0, 22.0, 19813.0, 6.0, 3233.0, 159.0, 27212.0, 19130.0, 1320.0, 15268.0, 7.0, 175.0, 189.0, 27593.0, 1315.0, 198.0, 2110.0, 27135.0, 28538.0, 7309.0, 5791.0, 142.0, 41030.0, 119.0, 1996.0, 6836.0, 16383.0, 110.0, 327.0, 11512.0, 174.0, 504.0, 46162.0, 82.0, 125.0, 2076.0, 8916.0, 2240.0, 2485.0, 575.0, 17056.0, 1789.0, 172.0, 16762.0, 90.0, 51.0, 154.0, 16281.0, 6684.0, 179.0]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
# |7128.0    |[3417.0, 53189.0, 58415.0, 8742.0, 939.0, 5739.0, 744.0, 26526.0, 102710.0, 14532.0, 2894.0, 5408.0, 13647.0, 49199.0, 161137.0, 1998.0, 990.0, 32682.0, 4749.0, 1254.0, 447.0, 58784.0, 4399.0, 18977.0, 10586.0, 14732.0, 11345.0, 11217.0, 1048.0, 52775.0, 1407.0, 5897.0, 1720.0, 44017.0, 5340.0, 16463.0, 2244.0, 1951.0, 36655.0, 974.0, 11896.0, 5465.0, 1920.0, 820.0, 3620.0, 34519.0, 7124.0, 2079.0, 3628.0, 11699.0, 11293.0, 1863.0, 8949.0, 16950.0, 2479.0, 76167.0, 36238.0, 59349.0, 4473.0, 24636.0, 5378.0, 15452.0, 1185.0, 560.0, 24700.0, 22789.0, 62442.0, 55224.0, 8920.0, 500.0, 32888.0, 3153.0, 36436.0, 14277.0, 5003.0, 93276.0, 5908.0, 107496.0, 62749.0, 20760.0, 842.0, 2404.0, 26183.0, 9038.0, 7835.0, 4590.0, 24543.0, 5966.0, 65996.0, 1804.0, 4900.0, 1789.0, 526.0, 19386.0, 51.0, 2659.0, 41644.0, 5594.0, 2175.0, 13697.0, 3973.0, 2309.0, 775.0]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
# |979.0     |[5923.0, 39359.0, 654.0, 168319.0, 33.0, 16513.0, 8496.0, 35952.0, 42658.0, 54.0, 8822.0, 9829.0, 1028.0, 73886.0, 846.0, 12987.0, 375.0, 232.0, 1864.0, 4111.0, 13759.0, 5792.0, 303.0, 49956.0, 2634.0, 4828.0, 7353.0, 10143.0, 19735.0, 1448.0, 49.0, 1386.0, 5070.0, 18568.0, 11642.0, 128.0, 2345.0, 12254.0, 12.0, 19.0, 12828.0, 10.0, 94307.0, 101.0, 36479.0, 336.0, 7202.0, 229599.0, 100.0, 18319.0, 31909.0, 18357.0, 15124.0, 13.0, 529.0, 38697.0, 73799.0, 9107.0, 208.0, 1022.0, 44639.0, 9038.0, 18.0, 68240.0, 56059.0, 901.0, 837.0, 15.0, 11128.0, 213.0, 97332.0, 19500.0, 31401.0, 775.0, 127017.0, 6722.0, 20.0, 302.0, 12297.0, 87799.0, 27098.0, 2093.0, 5611.0, 34656.0, 42246.0, 17623.0, 2472.0, 48001.0, 7975.0, 21456.0, 30874.0, 316.0, 49660.0, 8351.0, 357.0, 475.0, 9821.0, 51121.0, 2275.0, 3046.0, 67890.0, 10212.0, 157.0, 43529.0, 77793.0, 29183.0, 10088.0, 951.0, 223.0, 29393.0, 1237.0, 17431.0, 5153.0, 5052.0, 32299.0, 6322.0, 142.0, 2211.0, 7924.0, 825.0, 7883.0, 14064.0, 11535.0, 6240.0, 8951.0, 989.0, 61.0, 573.0, 110.0, 2489.0, 31894.0, 1200.0, 632.0, 54234.0, 1221.0, 1236.0, 73273.0, 42969.0, 9576.0, 264.0, 140.0, 74.0, 43.0, 28030.0, 1112.0, 7630.0, 1261.0, 65.0, 11214.0, 124.0, 4778.0, 6089.0, 57783.0, 696.0, 222.0, 154756.0, 190.0, 360.0, 99.0, 1804.0, 9917.0, 21537.0, 51.0, 90.0, 59938.0, 8811.0, 1111.0, 14454.0, 9589.0, 1011.0]                                                                                                                                                                                                                                                                                                   |
# |25836.0   |[211317.0, 208947.0, 75612.0, 131950.0, 3992.0, 108883.0, 52854.0, 220033.0, 7729.0, 5920.0, 6557.0, 3644.0, 45007.0, 61015.0, 45000.0, 87379.0, 5171.0, 13584.0, 41749.0, 167735.0, 210001.0, 20796.0, 9843.0, 537.0, 55217.0, 48142.0, 21927.0, 3076.0, 23832.0, 4540.0, 223403.0, 76470.0, 200981.0, 4993.0, 43941.0, 46859.0, 40852.0, 7609.0, 430.0, 12756.0, 111801.0, 282004.0, 12013.0, 40615.0, 97351.0, 51363.0, 28723.0, 181113.0, 40302.0, 6303.0, 50386.0]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
# +----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+


for user in [j['user_index'] for j in recommended_songs_chosen.collect()]:

    rec_songs = recommended_songs_chosen.filter(F.col('user_index')==user).collect()[0][1]
    act_songs = songs_played_chosen.filter(F.col('user_index')==user).collect()[0][1]
    count = 0
    print(f"recommended songs played by user {user}:")

    for song in rec_songs:
        if song in act_songs:
            print(song)
            count += 1

    print(f"{count} recommended songs are songs that the user has played")
    print('')

# recommended songs played by user 25836:
# 0 recommended songs are songs that the user has played
#
# recommended songs played by user 2675:
# 5
# 7
# 2 recommended songs are songs that the user has played
#
# recommended songs played by user 346:
# 0 recommended songs are songs that the user has played
#
# recommended songs played by user 979:
# 0 recommended songs are songs that the user has played
#
# recommended songs played by user 4805:
# 0 recommended songs are songs that the user has played
#
# recommended songs played by user 10245:
# 0 recommended songs are songs that the user has played
#
# recommended songs played by user 7128:
# 0 recommended songs are songs that the user has played



song_data = triplets_cleaned.select('song_id', 'song_index')
song_data = song_data.distinct()
song_data = song_data.join(metadata.select('song_id', 'title'), on='song_id', how='left')
song_data.show()
# +------------------+----------+--------------------+
# |           song_id|song_index|               title|
# +------------------+----------+--------------------+
# |SOAAADE12A6D4F80CC|  210444.0|(I'm Gonna Start)...|
# |SOAABRF12AC468803F|  243264.0|Strollin' With Bones|
# |SOAABRM12A58A7B273|  343172.0|               On TV|
# |SOAACBK12A67ADD198|  174742.0|                When|
# |SOAACFT12A8C137EB8|  115877.0|               Fotos|
# |SOAACQI12AB017F874|  151675.0|Malcolm_ Malcolm ...|
# |SOAACXA12AB018A864|  110440.0|Everything But My...|
# |SOAADAS12A58A784EC|  134936.0| How to Start a Band|
# |SOAADJH12AB018BD30|  274195.0|Black Light (Albu...|
# |SOAADJL12AB0184AD2|  257669.0|      Shattered mind|
# |SOAAFEW12A8AE47AE5|  243270.0|My Chucks (Explicit)|
# |SOAAFJR12A8C138259|  186909.0|   Stone Soul Picnic|
# |SOAAFYH12A8C13717A|    5454.0|Jesus Loves You (...|
# |SOAAGQN12A6D4F4BBA|  243274.0|            Blue Sky|
# |SOAAHEW12A8C142E31|  101191.0|Amanecí Entre Tus...|
# |SOAAHKE12A6D4F8EB5|  243276.0|              Gricel|
# |SOAAISU12AF72A44D3|   12220.0|          Can't Stop|
# |SOAAJSF12AB017FDB6|  343183.0|      Scorched Earth|
# |SOAAJSO12A58A7AB05|  316009.0|The Warmest Night...|
# |SOAAKFK12AB018D918|  164461.0|           Circadies|
# +------------------+----------+--------------------+
# only showing top 20 rows


for user in [j['user_index'] for j in recommended_songs_chosen.collect()]:
    rec_songs = recommended_songs_chosen.filter(F.col('user_index') == user).collect()[0][1]
    names = []
    for songIndex in rec_songs:
        songName = song_data.filter(F.col('song_index')==songIndex).collect()[0][2]
        names.append(songName)
    print(f'Songs recommended to user {user}: {names}')

# Songs recommended to user 25836: ['Epilogue', 'Mr. Jones', "Don't Cry (Original)", 'Paradise City', 'Le Courage Des Oiseaux']
# Songs recommended to user 2675: ['Fireflies', 'Dog Days Are Over (Radio Edit)', 'Secrets', 'Use Somebody', 'OMG']
# Songs recommended to user 346: ['The Gift', 'The Maestro', 'Never Let You Go', "Ghosts 'n' Stuff (Original Instrumental Mix)", 'They Might Follow You']
# Songs recommended to user 7128: ['Make Her Say', 'Creep (Explicit)', 'Nine Times Out Of Ten (1998 Digital Remaster)', 'Karma Police', 'Seven Nation Army']
# Songs recommended to user 4805: ['Mr. Jones', 'Epilogue', "Don't Cry (Original)", 'Under Pressure', 'Have You Ever Seen The Rain']
# Songs recommended to user 10245: ['Home', 'Dog Days Are Over (Radio Edit)', 'Cosmic Love', "You've Got The Love", 'The Funeral (Album Version)']
# Songs recommended to user 979: ['Dog Days Are Over (Radio Edit)', 'The Scientist', 'Use Somebody', 'Yellow', 'Clocks']


triplets_cleaned.filter(F.col('user_index').isin(user_index_chosen)).select('user_id','user_index').distinct().show(10, False)
# +----------------------------------------+----------+
# |user_id                                 |user_index|
# +----------------------------------------+----------+
# |60daae612f7a04811100e79bdce4f681039474d0|10245.0   |
# |9cfd53a26a029f23b8822f0bd6bb390ef6d0bdfa|4805.0    |
# |f24a5cce697b295c846f475c71043f4104f0c71b|2675.0    |
# |e340ca3e3d4edb20ecd8b865426af36dce1e1c79|25836.0   |
# |9ff5b97655c42065ee682e6af8edc5c1613c827d|346.0     |
# |75e93109df4fa6481986b08e456589b7e773a1a5|979.0     |
# |aa0631159478016fad0cbd0031d4af028e5f15d6|7128.0    |
# +----------------------------------------+----------+




#Question 2c
from pyspark.mllib.evaluation import RankingMetrics

def extract_songs(x):
  x = sorted(x, key=lambda i: -i[1])
  return [i[0] for i in x]

extract_songs_udf = F.udf(lambda x: extract_songs(x), ArrayType(IntegerType()))

relevant_songs = (
  test_sr
  .select(
    F.col("user_index").cast(IntegerType()),
    F.col("song_index").cast(IntegerType()),
    F.col("plays").cast(IntegerType())
  )
  .groupBy('user_index')
  .agg(
    F.collect_list(
      F.array(
        F.col("song_index"),
        F.col("plays")
      )
    ).alias('relevance')
  )
  .withColumn("relevant_songs", extract_songs_udf(F.col("relevance")))
  .select("user_index", "relevant_songs")
)

relevant_songs.show(10, False)
# +----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# |user_index|relevant_songs                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
# +----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# |38        |[10485, 12604, 34257, 12892, 29261, 6134, 8459, 12183, 6726, 41291, 8883, 523, 461, 37806, 3613, 50762, 27320, 464, 27292, 72704, 45689, 1055, 1691, 1983, 1921, 40941, 18618, 36471, 5288, 42945, 232, 68936, 23105, 3354, 4663, 857, 4860, 1468, 13353, 2455, 86679, 120369, 26289, 135, 19181, 1127, 20225, 8361, 57443, 822, 2635, 8170, 3109, 68733, 17305, 3205, 8492, 26, 57546, 9765, 1257, 433, 3281, 67862, 418, 7325, 5565, 236, 82, 6432, 86183, 11422, 15654, 50546, 9920, 844, 17324, 29332, 51959, 107, 11996, 35509, 11997, 2172, 1111, 6016, 4092, 37438, 2663, 81692, 635, 37601, 11477, 238, 3494, 42384, 4511, 53801, 59353, 47433, 11869, 77326, 65010, 15463, 5360, 51587, 31008, 82012, 1034, 2866, 2220, 2040, 3751, 208258, 5012, 2449, 180, 90194, 2587, 119595, 43423, 134364, 10194, 4771, 155024, 9941, 26419, 532, 89, 3087, 4462, 28, 18765, 48625, 1284, 7651, 67172, 2038, 1455, 19298, 3932, 155835, 26435, 18007, 2920, 9657, 17919, 35130, 730, 194797, 96195, 5939, 33208, 3119, 65354, 994, 6464, 38343, 24668, 2392, 1836, 498, 66323, 964, 15667, 15601, 939, 17853, 11841, 23314, 5634, 37451, 16049, 15551, 9503, 12969, 37048, 145, 11057, 125153, 505, 16130, 30872, 11369, 19437, 268, 22076, 12952, 8755, 15552, 7780, 114595, 6644, 50220, 6536, 4983, 67852, 12348, 6750, 17504, 19735, 22816, 5280, 2106, 19043, 7905, 53534, 18993, 10574, 873, 7941, 4107, 78070, 97828, 3453, 10343, 21280, 979, 7802, 3155, 1828, 11076, 342, 34820, 14964, 40969, 40445, 34412, 24065, 25158, 4179, 14833, 1800, 11134, 7581, 38711, 40136, 17022, 6697, 1252, 1926, 1445, 13796, 76814, 28323, 7703, 3237, 19499, 19327, 184935, 310, 19696, 15561, 1628, 29025, 31034, 19190, 53323, 140171, 37823, 1856, 40483, 109990, 1652, 755, 19698, 2808, 17212, 32087, 570, 56376, 23697, 12646, 788, 29327, 13640, 32747, 4952, 6240, 3770, 57439, 46246, 28340, 23591, 3492, 4555, 38455, 860, 61019, 63058, 2585, 37122, 7039, 31898, 4168, 12467, 73780, 23654, 9021, 18568, 13251, 70345, 20913, 23023, 65739, 23126, 11071, 4934, 10981, 29518, 4303, 51716, 23781, 9562]|
# |273       |[47823, 68494, 80201, 342, 92913, 133277, 17171, 111529, 81534, 24669, 90754, 100419, 13688, 25525, 5858, 87439, 48352, 6229, 8965, 9203, 14208, 33550, 26215, 63168, 33216, 25488, 6290, 11784, 2676, 255, 213574, 13677, 26762, 802, 93, 249, 53846, 5646, 40976, 15057, 22198, 147, 121430, 136864, 3747, 4736, 143627, 39568, 40013, 67570, 67602, 15161, 94762, 90360, 58, 75102, 962, 4033, 10493, 30365, 66272, 4182, 57165, 18775, 139, 90519, 58909, 27208, 2847, 38814, 22971, 2553, 22280, 1456, 26603, 31808, 259, 2899, 90628, 13235, 11727, 28195, 4050, 36905, 19350, 3871, 72776, 557, 25130, 67304, 21225, 15670, 28376, 379, 18693, 5323, 2333, 7257, 27156, 29997, 8815, 17420, 14372, 69988, 5045, 20568, 31505, 4068, 5744, 71654, 4989, 94235, 723, 6832, 3638, 153288, 35701, 4449, 1765, 21784, 920, 79546, 2766, 24942, 60889, 37650, 17862, 17629, 65548, 32175, 5300, 82810, 25953, 62479, 3236, 11061, 42252, 59432, 79651, 2709, 33722, 713, 35864, 91112, 38899, 146643, 23758, 18382, 1354, 92201, 2363, 7043, 15383, 6854, 32526, 20183, 99493, 103650, 3694, 23582, 21687, 68057, 404, 32631, 93458, 63856, 24709, 32836, 22782, 4406, 2339, 20859, 39237, 5065, 134503, 13865, 17329, 85558, 12987, 50900, 1130, 12190, 518, 13782, 6446, 20959, 18725, 7254, 31142, 19957, 3304, 112078, 35907, 85641, 5319, 12118, 99806, 129381, 59565]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
# |300       |[17750, 3151, 29582, 261, 4608, 21418, 2764, 9056, 3929, 3053, 2247, 9337, 14999, 150720, 9513, 112169, 5342, 2186, 26789, 167767, 173292, 51139, 56742, 42846, 20331, 34211, 419, 20021, 42892, 17680, 80, 11163, 29441, 39627, 5896, 1511, 1919, 888, 32606, 8215, 53281, 71, 22326, 1194, 13136, 9672, 58, 277, 1647, 16046, 7949, 2712, 12427, 241, 132316, 37867, 51471, 48935, 2962, 7243, 55503, 3252, 282, 36058, 4124, 9332, 17069, 5586, 69323, 3145, 48151, 100051, 27300, 3271, 78580, 552, 38831, 34640, 34380, 3041, 6299, 30383, 188658, 29552, 213, 45725, 20784, 100202, 2020, 30476, 24989, 89695, 80155, 242, 18066, 62364, 1323, 43318, 105, 15860, 130355, 5058, 9042, 1344, 75314, 4623, 283, 6542, 149355, 40280, 10758, 14448, 13238, 149565, 133375, 89870, 3987, 5413, 382, 1521, 303, 84373, 21493, 3254, 15023, 82, 237157, 172108, 7635, 69511, 1325, 459, 19225, 63745, 20710, 95533, 200, 28665, 106440, 5087, 14410, 18242, 9756, 23811, 4464, 19551, 29305, 14655, 344, 2157, 100740, 3605, 76828, 481, 15215, 4662, 185, 40654, 27035, 53014, 4864, 10694, 29117, 5673, 1383, 12545, 41201, 18519, 36275, 29408, 6859, 2741, 13328, 2273, 4524, 24848, 158927, 12216, 192539, 29412, 21566, 376, 90, 51142, 49615, 173753, 19609, 12255, 17663, 41758, 1902, 11174, 8183, 2954, 11474, 45844, 9085, 4910, 51677, 42, 15221, 14340, 1629, 121965, 69718, 5369, 90299, 68119, 16935, 1115, 9995, 4483, 3291, 10490, 35628, 30938, 67646, 9192, 2824, 466, 4998, 8488, 3498, 3765, 336, 508, 90427, 5302]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
# |412       |[249, 5602, 2473, 1559, 18880, 599, 34, 9044, 11416, 5860, 6513, 707, 1541, 12251, 713, 4764, 651, 4159, 229, 33651, 271, 43, 2016, 13591, 10968, 5076, 1843, 149, 2487, 501, 12352, 6182, 139976, 1537, 8165, 6577, 6403, 3719, 5180, 7253, 39588, 7044, 4709, 7397, 58, 49398, 8255, 6748, 53428, 66270, 9318, 3911, 6510, 6765, 14845, 16196, 19304, 3119, 37433, 6497, 15571, 52298, 60001, 18827, 47892, 28095, 11759, 21473, 1137, 27078, 19913, 30557, 8066, 338, 26753, 125069, 1475, 26839, 666, 4290, 19536, 1804, 52337, 19781, 60819, 9906, 4453, 296, 42399, 39511, 14479, 2803, 183042, 10497, 2354, 46802, 3819, 16170, 104693, 27421, 2476, 5156, 34676, 44937, 2896, 14379, 10891, 15789, 99135, 14864, 3532, 20221, 59412, 22139, 13007, 74175, 30888, 19363, 870, 5837, 73549, 13686, 153972, 29752, 2994, 12441, 1152, 2601, 4640, 5897, 1791, 178442, 227195, 49041, 2977, 31744, 23448, 21791, 16176, 3215, 32522, 4900, 36270, 5283, 12666, 7199, 1628, 1774, 17026, 17134, 41933, 17395, 4221, 59479, 3333, 2190, 9511, 34589, 7311, 4958, 15357, 6684, 34188, 56090, 103834, 27129, 7030, 9512, 4728, 18722, 12876, 32214, 23123, 8435, 4437, 47579, 46475, 9871, 3528, 6868, 37417, 5083, 8916, 737]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
# |434       |[17, 26, 5, 203, 59, 27160, 466, 27859, 4430, 36274, 4925, 882, 771, 2145, 18273, 12984, 35014, 381, 2718, 2351, 164, 471, 18749, 2316, 715, 66, 1115, 460, 79, 14216, 2418, 38, 5397, 534, 35499, 23836, 8633, 21, 60767, 4819, 4023, 34125, 977, 236, 22996, 1645, 4996, 103562, 2969, 2386, 147305, 42, 36548, 10979, 180574, 0, 43073, 176, 2056, 4628, 331, 21364, 2435, 15782, 76, 96414, 152, 4153, 10, 20516, 3865, 2, 7007, 110, 21743, 45, 17327, 33, 26805, 187, 89, 4441, 46, 25471, 75097, 58, 70383, 9749, 41986, 257, 5342, 1, 1116, 16664, 1017, 29891, 604, 11035, 434, 28359, 9151, 155, 12096, 40071, 39614, 91629, 27553, 15169, 62307, 478, 15255, 4, 613, 74, 356, 14849, 154, 1539, 10779, 20029, 6231, 15018, 9030, 1155, 1752, 20837, 66436, 72923, 13579, 27725, 15646, 627, 18028, 4962, 31100, 64617, 71717, 13209, 32057, 20404, 15902, 18658, 7476, 29106, 24, 39540, 1634, 125885, 2407, 28586, 6808, 15, 117184, 6321, 78210, 8217, 150403, 987, 10037, 2086, 6495, 69614, 50310, 134080, 35083, 2906, 7228, 23113, 52720, 10104, 67, 185316, 6486, 27, 5030, 3643, 1574, 7, 37545, 17836, 1311, 7279, 70311, 28, 3096, 90359, 35110, 2055, 50660, 39876, 79917, 83151, 13104, 35]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
# |475       |[69792, 4332, 150928, 845, 1047, 3084, 1423, 96542, 3478, 230, 852, 1157, 9378, 17305, 19687, 4038, 9836, 683, 23496, 82829, 458, 12253, 2522, 56440, 2653, 79182, 414, 29343, 60317, 2456, 699, 22116, 90509, 135165, 7784, 34889, 20873, 5831, 67739, 50957, 65836, 6288, 52299, 8948, 732, 4014, 37598, 97453, 17229, 43501, 68251, 492, 57864, 2960, 40243, 13920, 7022, 3970, 40761, 66347, 90697, 11220, 85025, 9960, 21716, 1935, 1912, 8203, 4120, 949, 27563, 50465, 28386, 15973, 841, 204941, 51820, 1500, 123083, 24053, 2940, 1488, 23983, 7973, 5434, 18744, 72254, 2728, 48756, 8395, 61613, 873, 65041, 9811, 65055, 51578, 89901, 15648, 4734, 1516, 82778, 9057, 200, 12412, 104875, 25003, 24, 11516, 43591, 162681, 5196, 207159, 299, 21239, 18080, 38566, 6809, 85335, 47498, 8208, 58378, 62935, 2648, 29018, 24234, 1688, 5898, 37815, 227927, 1628, 1005, 56029, 6264, 53331, 3673, 981, 134236, 868, 3592, 3611, 11230, 18524, 4611, 22663, 27266, 11633, 20420, 121736, 9734, 25341, 192591, 19807, 9901, 88284, 7607, 1591, 21993, 20774, 1738, 21796, 6970, 1730, 953, 140669, 38455, 131862, 27926, 18216, 18770, 1147, 273567, 66222, 57136, 40887, 13045, 19164, 79174]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
# |585       |[271, 172, 196, 5251, 8681, 2137, 2009, 1399, 60092, 20464, 8127, 24765, 7373, 6302, 12782, 343, 1377, 7608, 37567, 51988, 5192, 50, 24651, 12881, 31366, 8006, 187453, 393, 27547, 24452, 61480, 3163, 77828, 8172, 8578, 37876, 22068, 224, 15819, 31808, 9706, 3585, 964, 76539, 35146, 102887, 1533, 7068, 35152, 12774, 118384, 9795, 11888, 40578, 17047, 212986, 47179, 2035, 2110, 33900, 4800, 379, 1912, 8558, 54678, 35547, 13966, 9196, 949, 263030, 11391, 3958, 100312, 3281, 1701, 45526, 1241, 36479, 24051, 2388, 35971, 18948, 8358, 34399, 11116, 22082, 26855, 3064, 22031, 786, 65993, 6074, 2462, 2385, 30404, 4314, 18, 17627, 19181, 24064, 16143, 144, 6167, 28577, 870, 2809, 450, 1981, 8262, 37500, 2108, 23941, 13636, 2420, 9919, 47264, 20802, 5988, 18960, 139971, 12544, 3354, 34699, 5283, 21293, 14778, 26794, 5793, 20631, 34052, 30217, 21508, 76274, 7958, 420, 184, 2738, 16799, 55134, 113553, 9420, 3969, 11614, 40674, 16424, 2343, 10766, 1561, 330, 19563, 117642, 89323, 41768, 321, 17142, 1776, 32314, 40031, 95966, 5386, 9530, 7438, 23123, 56773, 5627, 1386, 53391, 58850, 66678, 28954, 6373, 32324, 29151, 6658]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
# |600       |[9541, 1157, 10768, 36953, 97424, 949, 4690, 4779, 23837, 72, 91, 94, 7044, 81, 895, 4943, 1299, 9103, 3078, 8178, 414, 8318, 16466, 7358, 44670, 108890, 4022, 23474, 7872, 5078, 14575, 29547, 8504, 4365, 42391, 841, 873, 5605, 9989, 39209, 1916, 6379, 7608, 3853, 15846, 40524, 14631, 3279, 7934, 5005, 2061, 15724, 548, 93815, 28790, 9836, 669, 13766, 17454, 68757, 61501, 9551, 11387, 26824, 125, 54626, 13723, 89577, 6046, 67263, 41831, 48425, 28885, 1108, 212327, 5438, 165469, 26833, 1686, 12841, 26676, 4953, 3089, 3008, 615, 642, 52597, 6207, 11268, 1085, 112758, 302, 60458, 1442, 6979, 4638, 1495, 74091, 17975, 145796, 1314, 76664, 2006, 698, 24610, 12247, 31101, 520, 20848, 8727, 3449, 31007, 5609, 461, 2933, 198358, 1258, 53885, 3142, 10872, 922, 543, 30413, 6089, 5446, 1099, 3903, 1121, 6982, 29310, 5405, 6675, 10592, 25812, 28234, 27353, 2376, 20679, 2897, 1856, 420, 12527, 2552, 1957, 289, 2434, 567, 40187, 218463, 4217, 22727, 6002, 1109, 8652, 11936, 3600, 1468, 2558, 137525, 1730, 287, 55765, 73755, 17871, 11578, 12029, 231, 26976, 5212, 12551, 71410, 219602, 25902, 2635, 1292, 90393, 26067, 10358, 427, 1317, 2788, 12424]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
# |611       |[3863, 292, 87, 698, 273, 792, 6892, 1909, 1222, 1211, 16550, 255, 777, 2027, 727, 271, 2573, 350, 1177, 224, 1015, 461, 576, 1161, 2397, 588, 4727, 1487, 1126, 1382, 410, 1127, 13349, 11471, 686, 1233, 2461, 1730, 2327, 2152, 4038, 1134, 53, 4978, 1777, 5690, 9221, 469, 4425, 1713, 1663, 4879, 6893, 1534, 1191, 957, 4184, 2713, 1558, 2334, 372, 876, 2337, 6332, 1189, 2053, 1802, 3354, 1223, 5181, 2400, 5363, 67568, 3259, 562, 532, 3164, 12902, 790, 3875, 3212, 8003, 925, 540, 17066, 594, 3901, 7784, 387, 2553, 6998, 3144, 3131, 4521, 18014, 4926, 1213, 2391, 1118, 1951, 8504, 4025, 1201, 1685, 1562, 8474, 4320, 13003, 351, 5069, 2417, 2667, 384, 15949, 29824, 2612, 9767, 13661, 4814, 5046, 4750, 10053, 8147, 5071, 3047, 5624, 2135, 1315, 783, 1000, 4083, 2786, 2874, 182, 4323, 6007, 2159, 1268, 5361, 571, 2648, 1426, 4724, 6222, 6053, 1962, 618, 2710, 624, 1304, 3054, 420, 184, 12547, 11633, 5752, 528, 2434, 2041, 6641, 4188, 3851, 4302, 5138, 5668, 9398, 2628, 5542, 2060, 1423, 5468, 4538, 9354, 2096, 7977, 18569, 14536, 12236, 2555, 737]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
# |619       |[2755, 223, 70692, 24, 24054, 27264, 56654, 12756, 84995, 64729, 26198, 19824, 14, 42947, 16675, 239685, 28136, 3, 87358, 16364, 14566, 5573, 152678, 21194, 123881, 26500, 114200, 7654, 9600, 33350, 13796, 17559, 21251, 2012, 156147, 32242, 40562, 21972, 68941, 139690, 61688, 42465, 33393, 31893, 93631, 18765, 27615, 2350, 19421, 1595, 220294, 2367, 243741, 3018, 88536, 8367, 41995, 3039, 99966, 43486, 156128, 160356, 43492, 70447, 232720, 165182, 40238, 6788, 30273, 102851, 28886, 27303, 8304, 1457, 891, 6011, 195786, 25637, 2655, 38203, 122824, 3051, 57576, 10666, 94067, 48720, 16231, 26299, 33903, 94096, 44908, 36213, 64089, 55587, 50466, 6142, 81019, 32149, 18583, 30294, 153045, 35427, 53827, 34932, 1432, 66928, 6761, 10550, 21492, 5488, 54434, 3006, 25504, 20847, 94324, 10253, 35441, 28664, 43579, 7734, 215896, 3151, 106475, 136662, 11695, 67978, 55680, 8583, 207029, 1641, 55096, 150224, 16102, 199052, 8129, 24623, 29939, 268705, 11334, 239342, 10559, 30115, 8742, 42266, 10925, 168009, 66584, 19190, 52719, 163280, 126306, 5465, 35737, 7992, 128891, 12417, 73131, 7791, 147344, 4218, 99662, 82244, 31892, 18131, 18810, 26655, 39119, 41425, 1216, 6885, 129321, 88401, 17843]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
# +----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# only showing top 10 rows


combined = (
  recommended_songs.join(relevant_songs, on='user_index', how='inner')
  .rdd
  .map(lambda row: (row[1], row[2]))
)

rankingMetrics = RankingMetrics(combined)



k=10

precisionAtK = rankingMetrics.precisionAt(k)
print('precision_at_10:     {:.4f}'.format(precisionAtK))
# precision_at_10:     0.0224

ndcgAtK = rankingMetrics.ndcgAt(k)
print('NDCG_at_10:          {:.4f}'.format(ndcgAtK))
# NDCG_at_10:          0.0329

MAP = rankingMetrics.meanAveragePrecision
print('MAP:                 {:.4f}'.format(MAP))
# MAP:                 0.0097