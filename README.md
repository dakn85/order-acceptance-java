# Gauge Java sample

Kleines ausführbares Gauge-Projekt für die Java-Kompatibilitätsprüfung.

Voraussetzungen für einen direkten lokalen Lauf: JDK 17+, Maven, Gauge und das
Java-Plugin (`gauge install java`). Der KnippQAi-Runner selbst verwendet Java 25
LTS, kann aber weiterhin Projekte mit niedrigerem `--release` ausführen.

```sh
gauge run specs
```

KnippQAi erkennt Specs, Szenarien und Java-`@Step`-Bindings, kompiliert Maven-
oder Gradle-Projekte in einem isolierten Runner-Workspace und löst den gesamten
Test-Classpath selbst auf. Das Projekt benötigt dafür weder KnippQAi-Hooks noch
zusätzliche Dependencies oder Konfigurationsdateien. Der Plattform-Agent wird
über `JAVA_TOOL_OPTIONS` zugeschaltet und erzeugt Evidence sowie Laufzeitmetriken.
