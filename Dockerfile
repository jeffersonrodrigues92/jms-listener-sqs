	FROM amazoncorretto:latest

	ENV jar_file=/target/jms-listener-0.0.1-SNAPSHOT.jar
	ENV wordir=/opt

	WORKDIR ${wordir}

	COPY ${jar_file} ${wordir}/jms-listener.jar

	ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","jms-listener.jar"]