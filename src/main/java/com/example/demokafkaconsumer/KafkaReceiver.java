package com.example.demokafkaconsumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.example.model.Employee;
import com.example.model.Student;

@Service
public class KafkaReceiver {

	@KafkaListener(topics="${kafka.topic.name}",groupId="${kafka.consumer.group.id}",containerFactory = "kafkaListenerContainerFactory")
	public void receiveData(Student student) {
		System.out.println(student.getId());
		System.out.println(student.getName());
	}
	
	@KafkaListener(topics="${kafka.topic.name.employee}",groupId="${kafka.consumer.group.id}",containerFactory = "employeeKafkaListenerContainerFactory")
	public void receiveDataEmployee(Employee emp) {
		System.out.println(emp.getName());
	}
}
