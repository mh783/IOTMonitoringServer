from datetime import datetime
from django.utils import timezone
from typing import Tuple
from django.contrib.auth.models import User
import requests
from receiver.models import Location, Station, Measurement, Data, City, State, Country
from django.db.models import Avg
from django.utils import timezone
from datetime import timedelta
import paho.mqtt.client as mqtt
UNITS = {
    "temperatura": "°C",
    "humedad": "%",
    "presion": "hPa",
    "luminosidad": "lx",
}


def get_coordinates(city: str, state: str, country: str) -> Tuple[float, float]:
    '''
    Servicio para conseguir las coordenadas de un lugar (nombre) usando GeoCode.xyz.
    '''
    lat = None
    lng = None
    city = ' '.join(city.split('+'))
    state = ' '.join(state.split('+'))
    country = ' '.join(country.split('+'))
    url = f'https://geocode.xyz/{city},{state},{country}?json=1'

    r = requests.get(url)
    if r.status_code == 200:
        lat = r.json().get('latt', 0)
        lng = r.json().get('longt', 0)
        lat = float(lat)
        lng = float(lng)
    return lat, lng


def get_units(variable: str) -> str:
    """
    Obtiene las unidades de una variable.
    """
    return UNITS.get(variable, '')


def get_topic_data(topic: str) -> Tuple[str, str, str, str]:
    """
    Obtiene los datos de un tópico.
    """
    try:
        parts = topic.split('/')
        country = parts[0]
        state = parts[1]
        city = parts[2]
        user = parts[3]
        topic_type = parts[4]
        if len(parts) > 5:
            raise Exception("Tópico incorrecto")
        return country, state, city, user
    except Exception as e:
        raise Exception('Tópico no válido: {}'.format(topic))


def get_user(username):
    '''
    Intenta traer el usuario con username {username}. Si no existe lanza una excepción.
    '''
    try:
        user = User.objects.get(username=username)
    except User.DoesNotExist:
        raise Exception(f'El usuario {username} no existe')
    return user


def get_or_create_location(city, state, country):
    '''
    Intenta traer la locación con nombre de ciudad, estado y país {city, state, country}.
    Si no existe, calcula las coordenadas de esa ubicación, lo crea y lo retorna.
    '''
    cityO, created = City.objects.get_or_create(name=city)
    stateO, created = State.objects.get_or_create(name=state)
    countryO, created = Country.objects.get_or_create(name=country)
    loc, created = Location.objects.get_or_create(
        city=cityO, state=stateO, country=countryO)
    if created:
        lat, lng = get_coordinates(city, state, country)
        loc.lat = lat
        loc.lng = lng
        loc.save()

    return loc


def get_or_create_station(user, location):
    '''
    Intenta traer la estación con usuario y locación {user, location}. Si no existe la crea y la retorna.
    '''
    station, created = Station.objects.get_or_create(
        user=user, location=location)
    return(station)


def get_or_create_measurement(name, unit):
    '''
    Intenta traer la variable con nombre y unidad {name, unit}. Si no existe la crea y la retorna.
    '''
    measurement, created = Measurement.objects.get_or_create(
        name=name, unit=unit)
    return measurement


def create_data(
    value: float,
    station: Station,
    measure: Measurement,
    time: datetime = datetime.now(),
):
    '''
    Crea un nuevo dato con valor {value}, estación {station} y variable {measure}.
    Hace las operaciones necesarias para insertarlo en la base de datos con el patrón Blob.
    Calcula promedio, mínimo y máximo de los datos anteriores.
    '''

    base_time = datetime(time.year, time.month, time.day,
                         time.hour, tzinfo=time.tzinfo)
    ts = int(base_time.timestamp() * 1000000)
    print("Time:", time)
    secs = int(time.timestamp() % 3600)

    data, created = Data.objects.get_or_create(
        base_time=base_time, station=station, measurement=measure, defaults={
            "time": ts,
        }
    )

    if created:
        values = []
        times = []
    else:
        values = data.values
        times = data.times

    values.append(value)
    times.append(secs)

    length = len(times)

    data.max_value = max(values) if length > 0 else 0
    data.min_value = min(values) if length > 0 else 0
    data.avg_value = sum(values) / length if length > 0 else 0
    data.length = length

    data.values = values
    data.times = times

    def publish_alert(topic: str, message: str, host: str, port: int, user: str, password: str):
        client = mqtt.Client()
        client.username_pw_set(user, password)
        client.connect(host, port, 60)
        client.publish(topic, message)
        client.disconnect()
    data.save()

# --- EVENTO: promedio en ventana de tiempo ---
    THRESHOLD = 28.0          # umbral (ajústalo)
    WINDOW_MINUTES = 3        # ventana (ajústala)

# Solo aplicar el evento a TEMPERATURA (evita que hum. también dispare)
# Ajusta esto según cómo se llame tu Measurement (name / code / unit)
# Ejemplo común: measure.name == "temperatura"
    if getattr(measure, "name", "").lower() in ["temperatura", "temperature", "temp"]:

        since = timezone.now() - timezone.timedelta(minutes=WINDOW_MINUTES)
        print("ALERT enviada:", msg)

    # CONSULTA A BD: promedio de valores recientes (últimos WINDOW_MINUTES)
        avg_recent = (
            Data.objects
            .filter(station=station, measurement=measure, timestamp__gte=since)
            .aggregate(avg=Avg("avg_value"))  # o Avg("max_value") o Avg("min_value")
            .get("avg")
            
        )

        if avg_recent is not None and avg_recent > THRESHOLD:
            topic = f"colombia/cundinamarca/bogota/{station.user.username}/in"
            msg = f"ALERT: TempProm({WINDOW_MINUTES}m)={avg_recent:.1f} > {THRESHOLD}"

            publish_alert(   # usa tu función de publicar (la que ya hiciste)
                topic="colombia/cundinamarca/bogota/stark/in",
                message="ALERT: temperatura alta",
                host="44.212.33.211",   # o pon el string directamente
                port="1883",
                user="stark",
                password="arcReactor123"
            )
    station.last_activity = time
    station.save()
    return data
