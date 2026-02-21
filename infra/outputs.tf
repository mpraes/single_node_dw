output "ssh_private_key_pem" {
  description = "Chave privada SSH gerada automaticamente"
  value       = tls_private_key.vm_ssh.private_key_pem
  sensitive   = true
}

output "ssh_public_key_openssh" {
  description = "Chave pública SSH gerada automaticamente"
  value       = tls_private_key.vm_ssh.public_key_openssh
}

output "ssh_username" {
  description = "Usuário SSH para conexão"
  value       = local.ssh_user
}

output "vm_public_ip" {
  description = "IP público da VM criada"
  value = local.selected_cloud == "aws" ? (
    aws_instance.vm[0].public_ip
    ) : (
    azurerm_public_ip.vm[0].ip_address
  )
}

output "selected_cloud_provider" {
  description = "Cloud provider efetivamente selecionado"
  value       = local.selected_cloud
}
