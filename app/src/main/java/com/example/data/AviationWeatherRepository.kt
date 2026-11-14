package com.example.data

import com.example.domain.AviationWeatherParser
import com.example.model.AirportStation
import com.example.model.DecodedWeather
import java.util.Calendar
import java.util.TimeZone
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONArray

class AviationWeatherRepository {

  private val client = OkHttpClient.Builder()
    .connectTimeout(10, TimeUnit.SECONDS)
    .readTimeout(12, TimeUnit.SECONDS)
    .build()

  private val customServerClient = CustomMeteoServerClient()

  data class FetchResult(
    val weather: DecodedWeather,
    val resolvedStation: AirportStation,
    val logMessages: List<String>,
  )

  fun loadDemoWeather(station: AirportStation): FetchResult {
    val (metar, taf) = when (station.icao) {
      "UTAM" -> Pair(
        "METAR UTAM 141700Z 08004KT 6000 BKN230 11/07 Q1023 R88/CLRD70 NOSIG",
        "TAF UTAM 141630Z 1418/1518 09012KT 5000 SCT020 BKN100 TEMPO 1418/1424 07012KT 2000 BR BKN005 TEMPO 1500/1506 VRB04KT 0600 FG VV002 TEMPO 1506/1512 05014KT",
      )
      "UTAA" -> Pair(
        "METAR UTAA 141700Z 11007KT 9999 SCT045 BKN120 13/05 Q1022 R12L/CLRD70 NOSIG",
        "TAF UTAA 141630Z 1418/1518 11010KT 8000 SCT040 BKN100 TEMPO 1500/1506 3000 BR BKN012",
      )
      "UTAK" -> Pair(
        "METAR UTAK 141700Z 32014G22KT 8000 FEW025 SCT060 14/08 Q1019 R16L/CLRD75 NOSIG",
        "TAF UTAK 141630Z 1418/1518 33015G25KT 9999 SCT030 TEMPO 1418/1502 5000 -RA BKN018",
      )
      "UTAT" -> Pair(
        "METAR UTAT 141700Z 05008KT 4000 BR BKN018 08/06 Q1024 R04/CLRD68 NOSIG",
        "TAF UTAT 141630Z 1418/1518 06010KT 4000 BR BKN015 TEMPO 1500/1506 0800 FG BKN004",
      )
      "UTAV" -> Pair(
        "METAR UTAV 141700Z 14006KT 9999 FEW050 12/04 Q1021 R13/CLRD72 NOSIG",
        "TAF UTAV 141630Z 1418/1518 13008KT CAVOK TEMPO 1502/1508 6000 SCT035",
      )
      "UTAN" -> Pair(
        "METAR UTAN 141700Z 29011KT 9999 SCT040 15/06 Q1020 R16/CLRD75 NOSIG",
        "TAF UTAN 141630Z 1418/1518 30013KT 9999 SCT035 TEMPO 1500/1506 6000 SCT020",
      )
      "UTAE" -> Pair(
        "METAR UTAE 141700Z 14007KT 8000 FEW045 BKN110 12/05 Q1022 R16/CLRD72 NOSIG",
        "TAF UTAE 141630Z 1418/1518 15009KT 9999 SCT040 TEMPO 1500/1506 5000 BR BKN015",
      )
      else -> Pair(
        "METAR ${station.icao} 141700Z 08004KT 6000 BKN230 11/07 Q1023 R88/CLRD70 NOSIG",
        "TAF ${station.icao} 141630Z 1418/1518 09012KT 5000 SCT020 BKN100 NOSIG",
      )
    }

    val demoCal = Calendar.getInstance(TimeZone.getTimeZone("UTC")).apply {
      set(2026, Calendar.NOVEMBER, 14, 17, 11, 0)
    }

    val decoded = AviationWeatherParser.parse(
      rawMetarInput = metar,
      rawTafInput = taf,
      station = station,
      sourceLabel = "Демо-данные (локальный шаблон ${station.icao})",
      receivedTimeLocal = if (station.icao == "UTAM") "22:11" else null,
      calendarForSun = demoCal,
    )

    return FetchResult(
      weather = decoded,
      resolvedStation = station,
      logMessages = listOf(
        "Загружен демо-пакет для ${station.icao} (${station.city})",
        "METAR: $metar",
      ),
    )
  }

  suspend fun fetchLiveWeather(
    station: AirportStation,
    serverConfig: ServerConfig = ServerConfig(),
  ): Result<FetchResult> = withContext(Dispatchers.IO) {
    val logs = mutableListOf<String>()
    val icao = station.icao.trim().uppercase()
    var resolvedStation = station

    var customError: String? = null
    if (serverConfig.useCustomServer && serverConfig.url.isNotBlank()) {
      val outcome = customServerClient.fetchMetarAndTaf(icao, serverConfig)
      logs.addAll(outcome.logs)
      val customMetar = outcome.rawMetar
      if (!customMetar.isNullOrBlank()) {
        val finalTaf = outcome.rawTaf ?: "TAF $icao — нет данных"
        val decoded = AviationWeatherParser.parse(
          rawMetarInput = customMetar,
          rawTafInput = finalTaf,
          station = resolvedStation,
          sourceLabel = "сервер ${outcome.sourceHost}",
        )
        logs.add("Успешно декодирован $icao с ${outcome.sourceHost} (${decoded.observationTimeUtc})")
        return@withContext Result.success(FetchResult(decoded, resolvedStation, logs))
      } else {
        customError = outcome.errorMessage
        if (!serverConfig.fallbackToNoaa) {
          return@withContext Result.failure(
            IllegalStateException(customError ?: "Нет данных для $icao на сервере ${outcome.sourceHost}"),
          )
        }
        logs.add("Переход к резервному источнику aviationweather.gov…")
      }
    }

    try {
      val metarUrl = "https://aviationweather.gov/api/data/metar?ids=$icao&format=json"
      val tafUrl = "https://aviationweather.gov/api/data/taf?ids=$icao&format=json"
      logs.add("GET $metarUrl")

      val metarReq = Request.Builder()
        .url(metarUrl)
        .header("User-Agent", "MeteotabloAndroid/1.0")
        .build()

      var rawMetar: String? = null
      var rawTaf: String? = null

      client.newCall(metarReq).execute().use { resp ->
        logs.add("METAR HTTP ${resp.code}")
        if (resp.isSuccessful) {
          val body = resp.body?.string().orEmpty().trim()
          if (body.startsWith("[")) {
            val arr = JSONArray(body)
            if (arr.length() > 0) {
              val obj = arr.getJSONObject(0)
              rawMetar = obj.optString("rawOb").takeIf { it.isNotBlank() }
              val lat = obj.optDouble("lat", Double.NaN)
              val lon = obj.optDouble("lon", Double.NaN)
              val elev = obj.optInt("elev", Int.MIN_VALUE)
              val apiName = obj.optString("name")
              if (!lat.isNaN() && !lon.isNaN() && AirportCatalog.presets.none { it.icao == icao }) {
                resolvedStation = resolvedStation.copy(
                  latitude = lat,
                  longitude = lon,
                  elevationMeters = if (elev != Int.MIN_VALUE) elev else resolvedStation.elevationMeters,
                  name = apiName.ifBlank { resolvedStation.name },
                )
              }
            }
          }
        }
      }

      // Fallback to NOAA TGFTP raw station file if JSON returned empty
      if (rawMetar.isNullOrBlank()) {
        val noaaUrl = "https://tgftp.nws.noaa.gov/data/observations/metar/stations/$icao.TXT"
        logs.add("Fallback GET $noaaUrl")
        val noaaReq = Request.Builder()
          .url(noaaUrl)
          .header("User-Agent", "MeteotabloAndroid/1.0")
          .build()
        client.newCall(noaaReq).execute().use { resp ->
          logs.add("TGFTP HTTP ${resp.code}")
          if (resp.isSuccessful) {
            val lines = resp.body?.string().orEmpty()
              .lines()
              .map { it.trim() }
              .filter { it.isNotBlank() }
            rawMetar = lines.lastOrNull { it.contains(icao) } ?: lines.lastOrNull()
          }
        }
      }

      // Fetch TAF
      val tafReq = Request.Builder()
        .url(tafUrl)
        .header("User-Agent", "MeteotabloAndroid/1.0")
        .build()
      runCatching {
        client.newCall(tafReq).execute().use { resp ->
          logs.add("TAF HTTP ${resp.code}")
          if (resp.isSuccessful) {
            val body = resp.body?.string().orEmpty().trim()
            if (body.startsWith("[")) {
              val arr = JSONArray(body)
              if (arr.length() > 0) {
                rawTaf = arr.getJSONObject(0).optString("rawTAF").takeIf { it.isNotBlank() }
              }
            }
          }
        }
      }

      val finalMetar = rawMetar
      if (finalMetar.isNullOrBlank()) {
        val msg = customError ?: "Нет актуального METAR для $icao на сервере aviationweather.gov"
        return@withContext Result.failure(IllegalStateException(msg))
      }

      val finalTaf = rawTaf ?: "TAF $icao — нет данных"
      val decoded = AviationWeatherParser.parse(
        rawMetarInput = finalMetar,
        rawTafInput = finalTaf,
        station = resolvedStation,
        sourceLabel = "интернет (aviationweather.gov)",
      )
      logs.add("Успешно декодирован $icao (${decoded.observationTimeUtc})")
      Result.success(FetchResult(decoded, resolvedStation, logs))
    } catch (e: Exception) {
      val msg = customError ?: (e.message ?: e::class.simpleName ?: "Ошибка сети")
      logs.add("Ошибка сети: $msg")
      Result.failure(IllegalStateException(msg, e))
    }
  }
}
